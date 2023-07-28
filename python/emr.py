import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

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
  r=run_shell_command(f'aws emr add-steps --cluster-id j-3MQRNV0NX8O9Q --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[--class,org.opensearch.sql.SQLJob,s3://flint.dev.penghuo.us-west-2/lib/jdk8/sqlRunner.jar,"select 1"]')
  jr = json.loads(r)
  stepId=jr['StepIds'][0]
  print("add-steps: " + stepId)

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws emr describe-step --cluster-id j-3MQRNV0NX8O9Q --step-id {stepId}')
     jr = json.loads(r)
     state=jr['Step']['Status']['State']
     if state == 'COMPLETED':
        break
     time.sleep(1)
     print("describe-step: " + state)

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
