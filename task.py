from celery import Celery, signals
from pymongo import MongoClient
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_URL = "http://localhost:8012/process"
MAX_RETRY_COUNT = 3

app = Celery('tasks', 
             broker='amqp://guest:guest@localhost:5672//',
             backend='mongodb://localhost:27017/celery_results')
app.conf.update(
    broker_connection_retry_on_startup=True
)

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
        

@app.task(bind=True)
def process_task(self, data: str):
    try:
        next_task = lock_task(self.request.id)
        if not next_task:
            logger.info("No pending tasks found.")
            return None

        response = send_task_to_server(next_task, data)
        if response:
            complete_task(next_task, response)
            logger.info(f"Task {next_task['task_id']} completed successfully.")
            return {'status': 'success', 'task_id': next_task['task_id']}
    except Exception as e:
        handle_task_failure(self, next_task, e)
        return {'status': 'error', 'task_id': next_task.get('task_id') if next_task else None, 'error': str(e)}

def lock_task(request_id):
    return _tasks_collection.find_one_and_update(
        {
            'status': 'pending',
            'locked_at': None,
        },
        {
            '$set': {
                'status': 'processing',
                'locked_at': datetime.utcnow(),
                'locked_by': request_id
            }
        },
        sort=[('sequence', 1)],
        return_document=True
    )

def send_task_to_server(task, data):
    try:
        response = requests.post(SERVER_URL, json={"task_id": task['task_id'], "data": data}, timeout=12000)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to send task {task['task_id']} to server: {e}")

def complete_task(task, result):
    _tasks_collection.update_one(
        {'_id': task['_id'],
         'locked_by': task['locked_by']
         },
        {
            '$set': {
                'status': 'completed',
                'completed_at': datetime.utcnow(),
                'result': result.get('result', 'No result returned')
            }
        }
    )

def handle_task_failure(self, task, exception):
    if not task:
        logger.error(f"Task failed with no task object: {exception}")
        return
    retry_count = task.get('retry_count', 0)
    if retry_count + 1 > MAX_RETRY_COUNT:
        _tasks_collection.update_one(
            {'_id': task['_id']},
            {
                '$set': {
                    'status': 'failed',
                    'locked_at': None,
                    'locked_by': None,
                    'error_message': str(exception)
                }
            }
        )
        logger.error(f"Task {task['task_id']} permanently failed after {MAX_RETRY_COUNT} retries.")
    else:
        _tasks_collection.update_one(
            {'_id': task['_id']},
            {
                '$set': {
                    'status': 'pending',
                    'locked_at': None,
                    'locked_by': None
                },
                '$inc': {'retry_count': 1}
            }
        )
        process_task.apply_async(kwargs={"data": task['data'], "sequence": task['sequence']}, countdown=60)
        logger.error(f"Task {task['task_id']} failed. Retrying {retry_count + 1}/{MAX_RETRY_COUNT}: {exception}")