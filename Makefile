run_client:
	uvicorn client:app --reload --port 8010

run_server:
	celery -A server worker --loglevel=info