from celery import Celery
import time
from pymongo import MongoClient
from datetime import datetime
import pika

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['task_db']
tasks_collection = db['tasks']

app = Celery('tasks', 
             broker='amqp://guest:guest@localhost:5672//',
             backend='rpc://')

@app.task(bind=True)
def process_task(self, data: str):
    try:
        # 락을 이용해 다음 작업 가져오기
        next_task = tasks_collection.find_one_and_update(
            {
                'status': 'pending',
                'locked_at': None
            },
            {
                '$set': {
                    'status': 'processing',
                    'locked_at': datetime.utcnow(),
                    'locked_by': process_task.request.id
                }
            },
            sort=[('sequence', 1)],
            return_document=True
        )
        
        if next_task:
            try:
                time.sleep(10)  # 작업 시뮬레이션
                
                # 작업 완료 처리
                tasks_collection.update_one(
                    {
                        '_id': next_task['_id'],
                        'locked_by': process_task.request.id
                    },
                    {
                        '$set': {
                            'status': 'completed',
                            'completed_at': datetime.utcnow(),
                            'locked_at': None,
                            'locked_by': None,
                            'result': f"Processed: {next_task['data']}"
                        }
                    }
                )
                return {'status': 'success', 'task_id': next_task['task_id']}
                
            except Exception as e:
                # 실패시 상태 복구
                tasks_collection.update_one(
                    {'_id': next_task['_id']},
                    {
                        '$set': {
                            'status': 'pending',
                            'locked_at': None,
                            'locked_by': None
                        }
                    }
                )
                return {'status': 'error', 'error': str(e)}
            
        process_task.apply_async(countdown=1)
        return None
    
    except Exception as e:
        return {"status": "error", "error": str(e)}