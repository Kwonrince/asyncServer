from celery import Celery, signals
from pymongo import MongoClient
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB 설정
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["task_db"]
tasks_collection = db["tasks"]

# Celery 설정
app = Celery('tasks', 
             broker='amqp://guest:guest@localhost:5672//',
             backend='mongodb://localhost:27017/celery_results')
app.conf.update(
    broker_connection_retry_on_startup=True  # 시작 시 broker 연결 재시도 활성화
)

# MongoDB 연결을 저장할 전역 변수
_mongo_client = None
_db = None
_tasks_collection = None

@signals.worker_process_init.connect
def init_worker(**kwargs):
    """Worker 프로세스가 시작될 때 MongoDB 연결 초기화"""
    global _mongo_client, _db, _tasks_collection
    _mongo_client = MongoClient("mongodb://localhost:27017/")
    _db = _mongo_client["task_db"]
    _tasks_collection = _db["tasks"]
    logger.info("MongoDB connection initialized for worker")

@signals.worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """Worker 프로세스가 종료될 때 MongoDB 연결 정리"""
    global _mongo_client
    if _mongo_client:
        _mongo_client.close()
        logger.info("MongoDB connection closed for worker")

# Server URL
SERVER_URL = "http://localhost:8012/process"
MAX_RETRY_COUNT = 3

@app.task(bind=True)
def process_task(self, data: str, sequence: int):
    try:
        # MongoDB에서 다음 작업 가져오기
        next_task = _tasks_collection.find_one_and_update(
            {
                'status': 'pending',
                'locked_at': None,
                'sequence': sequence
            },
            {
                '$set': {
                    'status': 'processing',
                    'locked_at': datetime.utcnow(),
                    'locked_by': self.request.id
                }
            },
            return_document=True
        )

        if not next_task:
            logger.info("No pending tasks found.")
            return None

        # Server로 작업 전송
        try:
            response = requests.post(SERVER_URL, json={"task_id": next_task['task_id'], "data": data})
            response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
            result = response.json()

            # 작업 완료 처리
            _tasks_collection.update_one(
                {
                    '_id': next_task['_id'],
                    'locked_by': self.request.id
                },
                {
                    '$set': {
                        'status': 'completed',
                        'completed_at': datetime.utcnow(),
                        'locked_at': None,
                        # 'locked_by': None,
                        'result': result.get('result', 'No result returned')
                    }
                }
            )
            logger.info(f"Task {next_task['task_id']} completed successfully.")
            return {'status': 'success', 'task_id': next_task['task_id']}

        except Exception as e:
            # 작업 실패 복구 및 celery 태스크 재등록
            retry_count = next_task.get('retry_count', 0)
            if retry_count + 1 > MAX_RETRY_COUNT:
                _tasks_collection.update_one(
                    {'_id': next_task['_id']},
                    {
                        '$set': {
                            'status': 'failed',
                            'locked_at': None,
                            'locked_by': None,
                            'error_message': f"Max retries exceeded: {e}"
                        }
                    }
                )
                logger.error(f"Task {next_task['task_id']} permanently failed after {MAX_RETRY_COUNT} retries.")
                return {'status': 'failed', 'task_id': next_task['task_id'], 'error': str(e)}
            _tasks_collection.update_one(
                {'_id': next_task['_id']},
                {
                    '$set': {
                        'status': 'pending',
                        'locked_at': None,
                        'locked_by': None,
                    },
                    '$inc': {'retry_count': 1}  # 재시도 횟수 증가
                }
            )
            process_task.apply_async(kwargs={"data": next_task['data'], "sequence": next_task['sequence']}, countdown=60)
            logger.error(f"Task {next_task['task_id']} failed. Retrying {retry_count + 1}/{MAX_RETRY_COUNT}: {e}")
            return {'status': 'error', 'task_id': next_task['task_id'], 'retry_count': retry_count + 1, 'error': str(e)}

    except Exception as e:
        logger.error(f"Error processing task: {e}")
        return {"status": "error", "error": str(e)}
