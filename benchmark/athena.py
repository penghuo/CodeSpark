import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

benchmarkQueries = [
   "SELECT AVG(size) FROM default.athena_http_logs WHERE status = 403",
#    "SELECT AVG(size) FROM default.athena_http_logs WHERE status = 304",
#    "SELECT COUNT(*) FROM default.athena_http_logs WHERE size = 1005",
#    "SELECT COUNT(*) FROM default.athena_http_logs WHERE size = 1005 AND status = 304",
  # "SELECT clientip, COUNT(*) AS cnt FROM default.athena_http_logs WHERE status = 403 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.athena_http_logs WHERE status = 304 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.athena_http_logs WHERE size = 1005 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.athena_http_logs WHERE size = 1005 AND status = 304 GROUP BY clientip ORDER BY cnt DESC LIMIT 3"
]

EXECUTION_TIMES=10
timings = []

def run_shell_command(cmd):
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        # Handle error
        print(f"Failed to execute command: {cmd}, error: {stderr.decode()}")
        return None

    # If the command was successful, return the output
    return stdout.decode()

def executeQuery(query):
  # start-calculation
  
  start_time = time.time()
  r=run_shell_command(f'aws athena --region us-west-2 start-query-execution --query-string "{query}" --work-group "primary" --query-execution-context Database=default,Catalog=AwsDataCatalog')
  startJob_end_time = time.time()
  
  jr = json.loads(r)
  jobRunId=jr['QueryExecutionId']
  print(f"  start-job-run {jobRunId} took: {startJob_end_time - start_time} seconds")

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws athena --region us-west-2 get-query-execution --query-execution-id {jobRunId}')
     jr = json.loads(r)
     state=jr['QueryExecution']['Status']['State']
     t = time.localtime()
     current_time = time.strftime("%H:%M:%S", t)
     if state == 'SUCCEEDED':
        break
     print(f"[{current_time}] get-job-run: " + state)

  query_end_time = time.time()
  print("===================================================")
  print(query)
  print(f"Statement execution took: {query_end_time - start_time} seconds")
  print("===================================================")
  timings.append(query_end_time - start_time)

for query in benchmarkQueries:
    timings.clear()
    for _ in range(EXECUTION_TIMES):
        executeQuery(query)        
    print("===================================================")
    print(query)
    print(f"Statement execution p100 took: {np.max(timings) } seconds")
    print(f"Statement execution p90 took: {np.percentile(timings, 90) } seconds")
    print(f"Statement execution p75 took: {np.percentile(timings, 75) } seconds")
    print("===================================================")
