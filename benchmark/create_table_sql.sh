#!/bin/zsh

sql="CREATE EXTERNAL TABLE default.http_logs_streaming (\`@timestamp\` TIMESTAMP, clientip STRING, request STRING, status INT, size INT) USING json OPTIONS (path 's3://flint-data-for-integ-test/data/http_log/mock-streaming/mock-http-log/*', compression 'bzip2')"
applicationId=00fd0j1dk4qpqp0p
executionRole=arn:aws:iam::994131275414:role/emr-job-execution-role
role=arn:aws:iam::391316693269:role/FlintOpensearchServiceRole

aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-for-integ-test/code/benchmark/sql-job.jar","entryPointArguments":["'${sql}'"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-for-integ-test/code/benchmark/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar"}}'
