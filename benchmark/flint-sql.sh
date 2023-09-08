#!/bin/zsh

sql="select * from default.http_logs limit 1"
applicationId=00fd0j1dk4qpqp0p
executionRole=arn:aws:iam::994131275414:role/emr-job-execution-role
role=arn:aws:iam::391316693269:role/FlintOpensearchServiceRole

aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-for-integ-test/code/benchmark/sql-job.jar","entryPointArguments":["'${sql}'", "query_results"],"sparkSubmitParameters":"--class org.opensearch.sql.FlintJob --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-for-integ-test/code/benchmark/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar --conf spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT --conf spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host=search-flint-dp-benchmark-cf5crj5mj2kfzvgwdeynkxnefy.eu-west-1.es.amazonaws.com --conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https --conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=eu-west-1 --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions  --conf spark.executor.instances=30 --conf spark.dynamicAllocation.initialExecutors=30 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}'
