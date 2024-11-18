run_client:
	uvicorn client:app --reload --port 8010

run_server:
	# docker run -d -p 27017:27017 mongo
	# docker run -d --name rabbitmq \
    # -p 5672:5672 \
    # -p 15672:15672 \
    # rabbitmq:management
	celery -A server worker --loglevel=info