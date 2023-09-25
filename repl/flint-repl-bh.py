import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

query="SELECT clientip, COUNT(*) AS cnt FROM default.http_logs WHERE status = 403 GROUP BY clientip ORDER BY cnt DESC LIMIT 3"
timings = []

def clean_up_index():
    print('Start Index deleted')
    url = 'http://ec2-54-70-149-21.us-west-2.compute.amazonaws.com:9200/'

    requests.delete(f"{url}/query_submit")
    requests.put(f"{url}/query_submit")

def submit_query(queryId, query):
    url = f"http://ec2-54-70-149-21.us-west-2.compute.amazonaws.com:9200/query_submit/_doc/{queryId}/?refresh=wait_for"
    headers = {'Content-Type': 'application/json'}
    data = {
        "id": f"{queryId}",
        "status": "pending",
        "query": f"{query}"
    }
    requests.post(url, headers=headers, json=data)

def query_result(queryId):
    url = f"http://ec2-54-70-149-21.us-west-2.compute.amazonaws.com:9200/query_submit/_search"
    headers = {'Content-Type': 'application/json'}
    data = {
        "query": {
            "term": {
                "id": {
                    "value": f"{queryId}"
                }
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    result = response.json()
    status = result['hits']['hits'][0]['_source']['status']
    print(f"status:{status}")
    return status

def executeQuery(queryId, query):
  # start-calculation
  start_time = time.time()

  submit_query(queryId, query)

  # get-calculation-execution
  while True:
     status=query_result(queryId)
     if status == 'complete':
        break
     time.sleep(1)

  query_end_time = time.time()
  print("===================================================")
  print(f"Statement execution took: {query_end_time - start_time} seconds")
  print("===================================================")
  timings.append(query_end_time - start_time)

for _ in range(11, 11 + 20):
    executeQuery(_, query)

print("===================================================")
print(f"Statement execution P100 took: {np.max(timings) } seconds")
print(f"Statement execution P90 took: {np.percentile(timings, 90) } seconds")
print(f"Statement execution P75 took: {np.percentile(timings, 75) } seconds")
print("===================================================")

clean_up_index()
