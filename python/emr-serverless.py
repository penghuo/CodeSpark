import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

applicationId="00fc9qgmi20r560l"
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

def executeQuery():
  # start-calculation
  start_time = time.time()
  r=run_shell_command('aws emr-serverless start-job-run --region us-west-2 --endpoint https://emr-serverless-beta.us-west-2.amazonaws.com --application-id 00fckp3fijj46b0l --execution-role-arn arn:aws:iam::864921806989:role/AmazonEMR-ExecutionRole-1686075706078  --job-driver \'{"sparkSubmit": {"entryPoint": "s3://flint.dev.penghuo.us-west-2/lib/jdk8/sql-job.jar","entryPointArguments":["select 1"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=2g"}}\' --profile flintpenglivy')
  startJob_end_time = time.time()
  
  jr = json.loads(r)
  jobRunId=jr['jobRunId']
  print(f"  start-job-run {jobRunId} took: {startJob_end_time - start_time} seconds")

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws emr-serverless get-job-run --region us-west-2 --endpoint https://emr-serverless-beta.us-west-2.amazonaws.com --application-id 00fckp3fijj46b0l --job-run-id {jobRunId} --profile flintpenglivy')
     jr = json.loads(r)
     state=jr['jobRun']['state']
     t = time.localtime()
     current_time = time.strftime("%H:%M:%S", t)
     if state == 'SUCCESS':
        break
     print(f"[{current_time}] get-job-run: " + state)

  query_end_time = time.time()
  print("===================================================")
  print(f"Statement execution took: {query_end_time - start_time} seconds")
  print("===================================================")
  timings.append(query_end_time - start_time)

for _ in range(20):
    executeQuery()

print("===================================================")
print(f"Statement execution P100 took: {np.max(timings) } seconds")
print(f"Statement execution P90 took: {np.percentile(timings, 90) } seconds")
print(f"Statement execution P75 took: {np.percentile(timings, 75) } seconds")
print("===================================================")
