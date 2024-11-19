from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import logging
from task import process_task
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

def setup_indexes():
    existing_indexes = tasks_collection.index_information()

    # 필요한 인덱스 정의
    required_indexes = [
        {"fields": [("status", 1), ("sequence", 1)], "options": {}},
        {"fields": [("task_id", 1)], "options": {"unique": True}},
        {"fields": [("completed_at", 1)], "options": {"expireAfterSeconds": 604800}},  # 7일 TTL
    ]

    for index in required_indexes:
        index_name = "_".join([f"{field[0]}_{field[1]}" for field in index["fields"]])
        if index_name not in existing_indexes:
            tasks_collection.create_index(index["fields"], **index["options"])
            logger.info(f"Index {index_name} created successfully.")
        else:
            logger.info(f"Index {index_name} already exists.")

    logger.info("Index setup complete.")

@app.on_event("startup")
def startup_event():
    setup_indexes()

class ProcessRequest(BaseModel):
   data: str

@app.post("/process")
def process_data(request: ProcessRequest):
    try:
        # 시퀀스 카운터 증가 (락 사용)
        counter = tasks_collection.find_one_and_update(
            {"_id": "counter"},
            [
                {
                    "$set": {
                        "value": {
                            "$cond": [
                                { "$gte": ["$value", 999999] },  # 100만 되면 리셋
                                1,
                                { "$add": ["$value", 1] }
                            ]
                        }
                    }
                }
            ],
            upsert=True,
            return_document=True
        )
        sequence = counter["value"]

        task = process_task.delay(request.data, sequence)
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
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/result/{task_id}")
def get_result(task_id: str):
    result = tasks_collection.find_one({'task_id': task_id})
    if result and result.get('status') == 'completed':
        return {"status": "completed", "result": result.get('result')}
    return {"status": "processing"}

if __name__ == "__main__":
   uvicorn.run("client:app", host="0.0.0.0", port=8010)