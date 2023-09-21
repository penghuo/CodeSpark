import boto3
import threading
import time
import requests
import json
import uuid

username = 'x'
password = 'x'

applicationId = "00fd777k3k3ls20p"
queryRuntime=900
bucket_name = 'flint-data-dp-eu-west-1-beta'
prefix = 'data/http_log/mock_streaming/'

run_generator=True

# todo. using session api
s3 = boto3.session.Session(profile_name='benchmark_customer_profile').client('s3', region_name='eu-west-1')
emr_serverless = boto3.session.Session(profile_name='benchmark_service_profile').client('emr-serverless', region_name='eu-west-1')

generator_interval = 300

def clean_up_index():
    print('Start Index deleted')
    url = 'https://search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com'

    delete_index_request=f"{url}/flint_default_http_logs_streaming_skipping_index"
    response = requests.delete(delete_index_request, auth=(username, password))

    # Check the response
    if response.status_code == 200:
        print('Index deleted successfully')
    else:
        print(f'Error deleting index. Status code: {response.status_code}')
        print(response.text)

def clean_up_s3():
    # Initialize a marker for pagination
    marker = None

    # Loop through paginated results
    while True:
        # List objects with the specified prefix and marker
        if marker == None:
          objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        else:
          objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=marker)
        # If there are objects with the specified prefix, delete them
        if 'Contents' in objects:
            objects_to_delete = [{'Key': obj['Key']} for obj in objects['Contents']]
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        else:
            print(f"No objects found with prefix '{prefix}'.")

        # Check if there are more objects to paginate through
        if 'NextContinuationToken' in objects:
            marker = objects['NextContinuationToken']
        else:
            break    
    print("Cleaned up S3 bucket.")


def clean_up_stack():
    clean_up_s3()
    clean_up_index()
    

def copy_file(file_name):
    # Define the S3 key (path) where the file will be stored
    file_key = f"data/http_log/mock_streaming/{file_name}"

    # Upload the JSON file to S3
    s3.copy_object(
        CopySource={'Bucket': bucket_name, 'Key': "data/http_log/raw/documents-211998.json.bz2"}, Bucket=bucket_name, 
        Key=file_key)
    print(f"uploaded to S3 bucket '{bucket_name}' at path '{file_key}'")


def http_log_generator():
    # create 1 file in target bucket
    current_time = time.strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f"{current_time}.{0}.json.bz2"

    copy_file(file_name)

    # sleep 60s, wait for cluster bootstrap
    time.sleep(30)

    global run_generator
    while run_generator:
        N = 1
        while N > 0:
            # Get the current timestamp to use in the file name
            current_time = time.strftime("%Y-%m-%d-%H-%M-%S")
            file_name = f"{current_time}.{N}.json.bz2"

            copy_file(file_name)
            N = N - 1
        time.sleep(generator_interval)

def submit_job_run(executors):
    executionRole = "arn:aws:iam::270824043731:role/emr-job-execution-role"
    role = "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole"

    sql = "create skipping index on default.http_logs_streaming (status VALUE_SET) with (auto_refresh = true)"
    osDomain = "search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com"
    dimension = f"--conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances={executors}"

    response = emr_serverless.start_job_run(
        applicationId=applicationId,
        executionRoleArn=executionRole,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": "s3://flint-data-dp-eu-west-1-beta/code/flint/sql-job.jar",
                "entryPointArguments": [sql, "wait"],
                "sparkSubmitParameters": f"""--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={role} --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={role} --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn={role} --conf spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar,s3://flint-data-dp-eu-west-1-beta/code/flint/opensearch-spark-standalone_2.12-0.1.0-SNAPSHOT.jar --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host={osDomain} --conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https --conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=eu-west-1 --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory {dimension}"""
            }
        }
    )
    print(f"Start job run {response['jobRunId']}.")
    return response['jobRunId']

def cancel_job_run(jobRunId):
    # Cancel a running job run
    emr_serverless.cancel_job_run(applicationId=applicationId, jobRunId=jobRunId)
    print(f"Job run {jobRunId} cancelled.")

def get_billed_resource_utilization(jobRunId):
    # Get billed resource utilization for a job run
    response = emr_serverless.get_job_run(applicationId=applicationId, jobRunId=jobRunId)

    if 'billedResourceUtilization' in response['jobRun']:
        billing = {
            'vCPUHour': response['jobRun']['billedResourceUtilization']['vCPUHour'],
            'memoryGBHour': response['jobRun']['billedResourceUtilization']['memoryGBHour']
        }
        print(billing)
        return billing
    else:
        return {
            'vCPUHour': 0,
            'memoryGBHour': 0,
        }

def get_latency():
    url = "https://search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com/flint_default_http_logs_streaming_skipping_index/_search"
    headers = {'Content-Type': 'application/json'}
    data = {
        "size": 0,
        "aggs": {
            "latency": {
                "percentiles": {
                    "field": "latency"
                }
            }
        }
    }

    response = requests.post(url, headers=headers, json=data, auth=(username, password))
    result = response.json()
    latency = {
        '95.0': result['aggregations']['latency']['values']['95.0'],
        '75.0': result['aggregations']['latency']['values']['75.0']
    }
    print(f"latency:{latency}")

    return latency

def write_to_benchmark(job_id, dimension, benchmarkId):
    billing = get_billed_resource_utilization(job_id)
    latency=get_latency()

    cost = billing['vCPUHour'] * 0.052624 + billing['memoryGBHour'] * 0.0057785
    data={
        "benchmarkId": benchmarkId,
        "applicationId": applicationId,
        "jobId": job_id,
        "vCPUHour": billing['vCPUHour'],
        "memoryGBHour": billing['memoryGBHour'],
        "cost": cost,
        "95.0": latency['95.0'],
        "75.0": latency['75.0'],
        "dimension": dimension
    }
    json_data = json.dumps(data)
    print(json_data)

    url = 'https://search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com'
    headers = {'Content-Type': 'application/json'}
    username = 'admin'
    password = 'Admin@123'

    put_index_request=f"{url}/benchmark/_doc"
    response = requests.post(put_index_request, json=data, headers=headers, auth=(username, password))

    # Check the response
    if response.status_code == 200:
        print('Data successfully indexed.')
    else:
        print(f'Error indexing data. Status code: {response.status_code}')
        print(response.text)

def benchmark(executors, benchmarkId):
    global run_generator

    print(f'>>>>> Start benchmark {benchmarkId}. executors: {executors}\n\n')
    # Step 1: Clean up S3 bucket
    clean_up_stack()

    # Step 2: Launch HTTP log generator thread
    run_generator = True
    http_log_thread = threading.Thread(target=http_log_generator)
    http_log_thread.start()

    # Step 3: Submit job
    job_id = submit_job_run(executors)
    time.sleep(queryRuntime)
    cancel_job_run(job_id)

    run_generator = False

    # Step 4: Get billed resource utilization and write to OpenSearch benchmark index
    time.sleep(30)
    dimension = {
        "executors": executors,
        "dynamicAllocation": False
    }
    write_to_benchmark(job_id, dimension, benchmarkId)

    http_log_thread.join()

    print(f'\n\n<<<<< Stop benchmark {benchmarkId}. executors: {executors}')

def main():
    random_uuid = uuid.uuid4()
    benchmarkId=f"streaming-benchmark-{random_uuid}"

    executorsDimension = [3, 10, 30]
    for executors in executorsDimension:
        benchmark(executors, benchmarkId)

if __name__ == '__main__':
    main()
