#!/bin/zsh
# create streaming table. default.http_logs_streaming


sql="CREATE EXTERNAL TABLE default.http_logs_streaming (\`@timestamp\` TIMESTAMP, clientip STRING, request STRING, status INT) USING json OPTIONS (path 's3://flint-data-dp-eu-west-1-beta/data/http_log/mock_streaming/', compression 'bzip2')"
applicationId=00fd777k3k3ls20p
executionRole=arn:aws:iam::270824043731:role/emr-job-execution-role
role=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole
aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-dp-eu-west-1-beta/code/flint/sql-job.jar","entryPointArguments":["'${sql}'", "query_results"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar --conf spark.executor.instances=1"}}'
