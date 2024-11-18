setup:
	docker run -d -p 27017:27017 mongo
	docker run -d --name rabbitmq \
    -p 5672:5672 \
    -p 15672:15672 \
    rabbitmq:management

run_client:
	uvicorn client:app --reload --port 8010

run_celery:
	celery -A task worker --loglevel=info --concurrency=4

run_server:
	uvicorn server:app --reload --port 8012