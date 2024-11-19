import requests
from requests.auth import HTTPBasicAuth
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

url = "http://localhost:15672/api/queues"
auth = HTTPBasicAuth("guest", "guest")  # RabbitMQ 기본 인증 정보

response = requests.get(url, auth=auth)

if response.status_code == 200:
    queues = [queue["name"] for queue in response.json()]
    print("Queues:", queues)
else:
    print(f"Failed to fetch queues: {response.status_code} {response.text}")
    
for queue in queues:
    channel.queue_delete(queue)
    print(f"Deleted queue: {queue}")

connection.close()