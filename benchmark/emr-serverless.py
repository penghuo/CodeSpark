import subprocess
import json, requests, os, time
import numpy as np
from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

applicationId="00fd0j1dk4qpqp0p"
executionRole="arn:aws:iam::994131275414:role/emr-job-execution-role"
flintRole="arn:aws:iam::391316693269:role/FlintOpensearchServiceRole"
benchmarkQueries = [
   "SELECT AVG(size) FROM default.http_logs WHERE status = 403",
#    "SELECT AVG(size) FROM default.http_logs WHERE status = 304",
#    "SELECT COUNT(*) FROM default.http_logs WHERE size = 1005",
#    "SELECT COUNT(*) FROM default.http_logs WHERE size = 1005 AND status = 304",
  # "SELECT clientip, COUNT(*) AS cnt FROM default.http_logs WHERE status = 403 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.http_logs WHERE status = 304 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.http_logs WHERE size = 1005 GROUP BY clientip ORDER BY cnt DESC LIMIT 3",
#   "SELECT clientip, COUNT(*) AS cnt FROM default.http_logs WHERE size = 1005 AND status = 304 GROUP BY clientip ORDER BY cnt DESC LIMIT 3"
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
  driverConf=f'{{"sparkSubmit":{{"entryPoint": "s3://flint-data-for-integ-test/code/benchmark/sql-job.jar","entryPointArguments":["{query}", "stop"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn={flintRole} --conf spark.jars=s3://flint-data-for-integ-test/code/benchmark/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar"}}}}'
  # 30 executors and skipping index
  # driverConf=f'{{"sparkSubmit":{{"entryPoint": "s3://flint-data-for-integ-test/code/benchmark/sql-job.jar","entryPointArguments":["{query}", "stop"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn={flintRole} --conf spark.jars=s3://flint-data-for-integ-test/code/benchmark/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar --conf spark.executor.instances=30 --conf spark.dynamicAllocation.initialExecutors=30 --conf spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT --conf spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host=search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com --conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https --conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=eu-west-1 --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions"}}}}'
  # driverConf=f'{{"sparkSubmit":{{"entryPoint": "s3://flint-data-for-integ-test/code/benchmark/sql-job.jar","entryPointArguments":["{query}", "stop"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN={flintRole} --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn={flintRole} --conf spark.jars=s3://flint-data-for-integ-test/code/benchmark/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar --conf spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT --conf spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host=search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com --conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https --conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=eu-west-1 --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions"}}}}'  
  start_time = time.time()
  r=run_shell_command(f'aws emr-serverless start-job-run --region eu-west-1 --application-id {applicationId} --execution-role-arn {executionRole}  --job-driver \'{driverConf}\'')
  startJob_end_time = time.time()
  
  jr = json.loads(r)
  jobRunId=jr['jobRunId']
  print(f"  start-job-run {jobRunId} took: {startJob_end_time - start_time} seconds")

  # get-calculation-execution
  while True:
     r=run_shell_command(f'aws emr-serverless get-job-run --region eu-west-1 --application-id {applicationId} --job-run-id {jobRunId}')
     jr = json.loads(r)
     state=jr['jobRun']['state']
     t = time.localtime()
     current_time = time.strftime("%H:%M:%S", t)
     if state == 'SUCCESS':
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
