'''
Simple Python script to connect to EMR Serverless Livy endpoint using requests library
Requires
1. AWS credentials configured in appropriate location. For example, environment variables in the shell where this Python script will be called
2. host and execution_role below to be correctly set
'''

import json, requests, os, time
import numpy as np
import concurrent.futures
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# Configure the following
host = 'http://xxx.livy.emr-serverless-services-beta.us-west-2.amazonaws.com' # configure as required for application
execution_role = '' # provide valid execution role

# Creating AWS SigV4 signer
boto_session = session.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name="us-west-2"

)  # load default credentials and config
credentials = boto_session.get_credentials()
service_name = "emr-serverless"
region = os.getenv("AWS_REGION", "us-west-2")
aws_signer = SigV4Auth(credentials, service_name, region)

def get_headers(url, http_method, body):
    headers = {'Content-Type': 'application/json'}
    aws_request = AWSRequest(method=http_method, url=url, data=body, headers=headers)
    aws_signer.add_auth(aws_request)
    aws_request.headers['ExecutionRole'] = execution_role
    return aws_request.headers

def executeQuery(session_url):
    start_time = time.time()

    # ExecuteCode
    statement_url = session_url + '/statements'
    data = {'code': 'select 1'}
    print(statement_url)
    r = requests.post(statement_url, data=json.dumps(data), headers=get_headers(statement_url, 'POST', json.dumps(data)))
    print(r)
    print(r.content)
    if r.status_code == 201 or r.status_code == 200 :
        print('Statement id: ' + str(r.json()['id']))

    # GetCodeResult
    result_url = host + r.headers['location']
    print(result_url)
    r = requests.get(result_url, headers=get_headers(result_url, 'GET', None))
    print(r)
    print(r.content)
    if r.status_code == 201 or r.status_code == 200 :
        print('Statement state: ' + r.json()['state'])

    while r.json()['state'] != 'available' :
        time.sleep(1)
        r = requests.get(result_url, headers=get_headers(result_url, 'GET', None))

    end_time = time.time()
    took = end_time - start_time

    print("===================================================")
    print(f"Statement execution took: {took} seconds")
    print("===================================================")
    return took

start_time = time.time()

# Create a session
sessions_url = host + '/sessions'
data = {'kind': 'sql'}
print(sessions_url)
r = requests.post(sessions_url, data=json.dumps(data), headers=get_headers(sessions_url, 'POST', json.dumps(data)))
print(r)
print(r.content)
if r.status_code == 201 or r.status_code == 200 :
    print('Session id: ' + str(r.json()['id']))

# Get session
session_url = host + r.headers['location']
print(session_url)
r = requests.get(session_url, headers=get_headers(session_url, 'GET', None))
print(r)
print(r.content)
if r.status_code == 201 or r.status_code == 200 :
    print('Session state: ' + r.json()['state'])


# To execute code, wait till session is in idle state
while r.json()['state'] != 'idle' :
    time.sleep(1)
    r = requests.get(session_url, headers=get_headers(session_url, 'GET', None))
    
end_time = time.time()
execution_time = end_time - start_time
print("==============================================")
print(f"Create session took: {execution_time} seconds")
print("==============================================")

if r.status_code == 201 or r.status_code == 200 :
    print('Session state: ' + r.json()['state'])
else :
    print(r)


timings = []
for _ in range(20):
    start_time = time.time()
    executeQuery(session_url)
    end_time = time.time()
    print("===================================================")
    print(f"Statement execution took: {end_time - start_time} seconds")
    print("===================================================")

    timings.append(end_time - start_time)
p90 = np.percentile(timings, 90) 
print("===================================================")
print(f"Statement execution P90 took: {p90} seconds")
print("===================================================")


# Close the session
print(session_url)
r = requests.delete(session_url, headers=get_headers(session_url, 'DELETE', None))
print(r)
if r.status_code == 201 or r.status_code == 200 :
    print(r.json())
