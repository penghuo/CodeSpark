#!/bin/zsh
sql="CREATE EXTERNAL TABLE default.http_logs (\`@timestamp\` TIMESTAMP, clientip STRING, request STRING, status INT, size INT, year INT, month INT, day INT) USING json PARTITIONED BY(year, month, day) OPTIONS (path 's3://flint-data-dp-eu-west-1-beta/data/http_log/http_logs_partitioned_json_bz2/', compression 'bzip2')"
applicationId=00fd1ddomb5tp60p
executionRole=arn:aws:iam::270824043731:role/emr-job-execution-role
role=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole
aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-dp-eu-west-1-beta/code/flint/sql-job.jar","entryPointArguments":["'${sql}'", "query_results"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar"}}'
