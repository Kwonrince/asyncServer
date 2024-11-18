from fastapi import FastAPI
from pydantic import BaseModel
import time

app = FastAPI()

class ServerTask(BaseModel):
    data: str

@app.post("/process")
def process_task(task: ServerTask):
    # 작업 처리 시뮬레이션
    time.sleep(5)  # GPU 작업 처리 시뮬레이션
    return {"result": f"Processed: {task.data}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8012)