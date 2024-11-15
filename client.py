from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import logging
from server import process_task
from pymongo import MongoClient
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("uvicorn.access").addFilter(lambda record: "/result" not in record.getMessage())
logging.getLogger("uvicorn.access").addFilter(lambda record: "/process" not in record.getMessage())

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['task_db']
tasks_collection = db['tasks']
app = FastAPI()

class ProcessRequest(BaseModel):
   data: str

@app.post("/process")
def process_data(request: ProcessRequest):
   # 시퀀스 카운터 증가 (락 사용)
   counter = tasks_collection.find_one_and_update(
       {"_id": "counter"},
       {"$inc": {"value": 1}},
       upsert=True,
       return_document=True
   )
   sequence = counter["value"]
   
   task = process_task.delay(request.data)
   tasks_collection.insert_one({
        'task_id': task.id,
        'sequence': sequence,
        'status': 'pending',
        'data': request.data,
        'created_at': datetime.utcnow(),
        'completed_at': None,
        'locked_at': None,
        'locked_by': None
    })
   
   response = {"task_id": task.id, "data": request.data, "status": "queued"}
   logger.info(f"Process response: {response}")
   return response

@app.get("/result/{task_id}")
def get_result(task_id: str):
    result = tasks_collection.find_one({'task_id': task_id})
    if result and result.get('status') == 'completed':
        return {"status": "completed", "result": result.get('result')}
    return {"status": "processing"}

if __name__ == "__main__":
   uvicorn.run("client:app", host="0.0.0.0", port=8010)