#!/bin/zsh

sql="create skipping index on default.http_logs_streaming (status VALUE_SET) with (auto_refresh = true)"

applicationId=00fd777k3k3ls20p
executionRole=arn:aws:iam::270824043731:role/emr-job-execution-role
role=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole

osDomain=search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com
dimension="--conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances=10"

aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-dp-eu-west-1-beta/code/flint/sql-job.jar","entryPointArguments":["'${sql}'", "wait"],"sparkSubmitParameters":"--class org.opensearch.sql.SQLJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar,s3://flint-data-dp-eu-west-1-beta/code/flint/opensearch-spark-standalone_2.12-0.1.0-SNAPSHOT.jar --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host='${osDomain}' --conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https --conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=eu-west-1 --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory '${dimension}'"}}'
