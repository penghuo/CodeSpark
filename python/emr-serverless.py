import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

applicationId="00fc9qgmi20r560l"

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
  r=run_shell_command('aws emr-serverless start-job-run --application-id 00fc9qgmi20r560l --execution-role-arn arn:aws:iam::483560928347:role/FlintEMRServerlessS3RuntimeRole  --job-driver \'{"sparkSubmit": {"entryPoint": "s3://flint.dev.penghuo.us-west-2/lib/jdk8/sql-job.jar","entryPointArguments":["select 1"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=2g"}}\' --profile flintappsec')
  jr = json.loads(r)
  jobRunId=jr['jobRunId']
  print("start-job-run: " + jobRunId)

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws emr-serverless get-job-run --application-id 00fc9qgmi20r560l --job-run-id {jobRunId}')
     jr = json.loads(r)
     state=jr['jobRun']['state']
     if state == 'SUCCESS':
        break
     time.sleep(1)
     print("get-job-run: " + state)

timings = []
for _ in range(20):
    start_time = time.time()
    executeQuery()
    end_time = time.time()
    print("===================================================")
    print(f"Statement execution took: {end_time - start_time} seconds")
    print("===================================================")

    timings.append(end_time - start_time)

print("===================================================")
print(f"Statement execution P100 took: {np.max(timings) } seconds")
print(f"Statement execution P90 took: {np.percentile(timings, 90) } seconds")
print(f"Statement execution P75 took: {np.percentile(timings, 75) } seconds")
print("===================================================")
