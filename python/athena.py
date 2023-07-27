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

def executeQuery(sessionId):
  # start-calculation
  r=run_shell_command(f'aws athena start-calculation-execution --session-id {sessionId} --code-block \'spark.sql("select 1").show()\'')
  jr = json.loads(r)
  calculationExecutionId=jr['CalculationExecutionId']
  print("start-calculation-execution: " + jr['State'])

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws athena get-calculation-execution-status --calculation-execution-id {calculationExecutionId}')
     jr = json.loads(r)
     if jr['Status']['State'] == 'COMPLETED':
        print("SubmissionDateTime: " + jr['Status']['SubmissionDateTime'])
        print("CompletionDateTime: " + jr['Status']['CompletionDateTime'])
        break
     time.sleep(1)
     print("get-calculation-execution-status: " + jr['Status']['State'])


start_time = time.time()
# start-session
r=run_shell_command('aws athena start-session --work-group flint-pdx --engine-configuration \'{"CoordinatorDpuSize": 1, "MaxConcurrentDpus":20, "DefaultExecutorDpuSize": 1}\'')
jr = json.loads(r)
sessionId=jr['SessionId']
print("sessionId: " + sessionId)
print("start-session: " + jr['State'])

# wait until session idle
while True:
  r=run_shell_command('aws athena get-session-status --session-id ' + sessionId)
  jr = json.loads(r)
  if jr['Status']['State'] == 'IDLE':
      break
  print("get-session-status: " + jr['Status']['State'])
  time.sleep(1)

end_time = time.time()
execution_time = end_time - start_time
print("==============================================")
print(f"start session took: {execution_time} seconds")
print("==============================================")

timings = []
for _ in range(20):
    start_time = time.time()
    executeQuery(sessionId)
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



# terminate-session
r=run_shell_command(f'aws athena terminate-session --session-id {sessionId}')
jr = json.loads(r)
print("terminate-session: " + jr['State'])
