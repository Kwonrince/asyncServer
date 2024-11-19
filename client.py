from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import logging
from task import process_task
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("uvicorn.access").addFilter(lambda record: "/result" not in record.getMessage())
logging.getLogger("uvicorn.access").addFilter(lambda record: "/process" not in record.getMessage())

mongo_client = AsyncIOMotorClient('mongodb://localhost:27017/')
db = mongo_client['task_db']
tasks_collection = db['tasks']
app = FastAPI()

async def setup_indexes():
    existing_indexes = await tasks_collection.index_information()

    # 필요한 인덱스 정의
    required_indexes = [
        {"fields": [("status", 1), ("sequence", 1)], "options": {}},
        {"fields": [("task_id", 1)], "options": {"unique": True}},
        {"fields": [("completed_at", 1)], "options": {"expireAfterSeconds": 604800}},  # 7일 TTL
    ]

    for index in required_indexes:
        index_name = "_".join([f"{field[0]}_{field[1]}" for field in index["fields"]])
        if index_name not in existing_indexes:
            await tasks_collection.create_index(index["fields"], **index["options"])
            logger.info(f"Index {index_name} created successfully.")
        else:
            logger.info(f"Index {index_name} already exists.")

    logger.info("Index setup complete.")

@app.on_event("startup")
async def startup_event():
    await setup_indexes()

class ProcessRequest(BaseModel):
   data: str

@app.post("/process")
async def process_data(request: ProcessRequest):
    try:
        # 시퀀스 카운터 증가 (락 사용)
        current = await tasks_collection.find_one({"_id": "counter"})
        if current and current.get("value", 0) >= 999999:
            counter = tasks_collection.find_one_and_update(
                {"_id": "counter"},
                {"$set": {"value": 1}},
                return_document=True,
                upsert=True
            )
        else:
            counter = await tasks_collection.find_one_and_update(
                {"_id": "counter"},
                {"$inc": {"value": 1}},
                return_document=True,
                upsert=True
            )
        sequence = counter["value"]

        # Dramatiq 태스크 등록
        task_id = str(uuid.uuid4())
        await tasks_collection.insert_one({
            'task_id': task_id,
            'sequence': sequence,
            'status': 'pending',
            'data': request.data,
            'created_at': datetime.utcnow(),
            'completed_at': None,
            'locked_at': None,
            'locked_by': None
        })
        process_task.send(request.data, task_id)

        response = {"task_id": task_id, "data": request.data, "status": "queued"}
        logger.info(f"Process response: {response}")
        return response
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/result/{task_id}")
async def get_result(task_id: str):
    result = await tasks_collection.find_one({'task_id': task_id})
    if result and result.get('status') == 'completed':
        return {"status": "completed", "result": result.get('result')}
    return {"status": "processing"}

if __name__ == "__main__":
   uvicorn.run("client:app", host="0.0.0.0", port=8010)