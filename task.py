import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import AsyncIO, Middleware
from motor.motor_asyncio import AsyncIOMotorClient
import requests
from datetime import datetime
import logging
import asyncio
import httpx

class MessageTrackingMiddleware(Middleware):
    def before_process_message(self, broker, message):
        # 메시지 ID를 컨텍스트에 저장
        self.current_message_id = message.message_id

    def after_process_message(self, broker, message, *, result=None, exception=None):
        # 메시지 ID를 정리
        self.current_message_id = None

    def get_message_id(self):
        return getattr(self, "current_message_id", None)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB 설정
mongo_client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = mongo_client["task_db"]
tasks_collection = db["tasks"]

# RabbitMQ 브로커 설정
rabbitmq_broker = RabbitmqBroker(url="amqp://guest:guest@localhost:5672")
rabbitmq_broker.add_middleware(AsyncIO())
rabbitmq_broker.add_middleware(MessageTrackingMiddleware())
dramatiq.set_broker(rabbitmq_broker)

# 미들웨어 확인
async def get_message_id_from_middleware():
    for middleware in rabbitmq_broker.middleware:
        if isinstance(middleware, MessageTrackingMiddleware):
            return middleware.get_message_id()
    logger.error("MessageTrackingMiddleware not found.")
    return None
        
# Server URL
SERVER_URL = "http://localhost:8012/process"
MAX_RETRY_COUNT = 3

@dramatiq.actor
async def process_task(data: str, task_id: str):
    try:
        # MongoDB에서 다음 작업 가져오기
        message_id = await get_message_id_from_middleware()
        next_task = await tasks_collection.find_one_and_update(
            {
                'status': 'pending',
                'locked_at': None,
                'task_id': task_id
            },
            {
                '$set': {
                    'status': 'processing',
                    'locked_at': datetime.utcnow(),
                    'locked_by': message_id
                }
            },
            return_document=True
        )

        if not next_task:
            logger.info("No pending tasks found.")
            return None

        # Server로 작업 전송
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                response = await client.post(SERVER_URL, json={"task_id": next_task['task_id'], "data": data})
                response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
                result = response.json()

            # 작업 완료 처리
            await tasks_collection.update_one(
                {
                    '_id': next_task['_id'],
                    'locked_by': message_id
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
            return None

        except Exception as e:
            # 작업 실패 복구 및 celery 태스크 재등록
            retry_count = next_task.get('retry_count', 0)
            if retry_count + 1 > MAX_RETRY_COUNT:
                await tasks_collection.update_one(
                    {'_id': next_task['_id']},
                    {
                        '$set': {
                            'status': 'failed',
                            'locked_at': None,
                            'error_message': f"Max retries exceeded: {e}"
                        }
                    }
                )
                logger.error(f"Task {next_task['task_id']} permanently failed after {MAX_RETRY_COUNT} retries.")
                return {'status': 'failed', 'task_id': next_task['task_id'], 'error': str(e)}
            else:
                await tasks_collection.update_one(
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
                process_task.send_with_options(args=(data, task_id), delay=60000)
                logger.error(f"Task {next_task['task_id']} failed. Retrying {retry_count + 1}/{MAX_RETRY_COUNT}: {e}")
                return {'status': 'error', 'task_id': next_task['task_id'], 'retry_count': retry_count + 1, 'error': str(e)}

    except Exception as e:
        logger.error(f"Error processing task: {e}")
        return {"status": "error", "error": str(e)}
