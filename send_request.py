import concurrent.futures
import requests
import backoff
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RequestClient:
    def __init__(self, base_url: str, max_workers: int = 4):
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
    def send_request(self, data: str) -> dict:
        try:
            # 작업 요청
            response = self.session.post(f'{self.base_url}/process', json={"data": data}, timeout=12000000)
            task_id = response.json()['task_id']
            logger.info(f"Task queued - data: {data} | id: {task_id}")
            return {"data": data, "task_id": task_id}
                
        except Exception as e:
            logger.error(f"Failed to process data: {data}, error: {str(e)}")
            return {"data": data, "error": str(e)}

    def process_multiple(self, data_list: List[str]):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.send_request, data) for data in data_list]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        return results

if __name__ == "__main__":
    client = RequestClient("http://localhost:8010", max_workers=4)
    data_list = [f"test_data_{i}" for i in range(41,60)]
    results = client.process_multiple(data_list)
    
    for result in results:
        logging.info(result)