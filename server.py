from fastapi import FastAPI
from pydantic import BaseModel
import time
import logging
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("uvicorn.access").addFilter(lambda record: "/process" not in record.getMessage())

app = FastAPI()

class ServerTask(BaseModel):
    task_id: str
    data: str

@app.post("/process")
def process_task(task: ServerTask):
    # 작업 처리 시뮬레이션
    random_time = random.randint(2,6)
    time.sleep(random_time)  # GPU 작업 처리 시뮬레이션
    logger.info(f"Task {task.task_id} completed successfully.")
    return {"result": f"Processed: {task.data} with {random_time}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8012)