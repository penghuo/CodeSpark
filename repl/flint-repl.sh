#!/bin/zsh

applicationId=00fd775baqpu4g0p
executionRole=arn:aws:iam::270824043731:role/emr-job-execution-role
role=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole

osDomain=ec2-54-70-149-21.us-west-2.compute.amazonaws.com

aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-data-dp-eu-west-1-beta/code/flint/opensearch-spark-standalone_2.12-0.1.0-SNAPSHOT.jar","entryPointArguments":["query_submit", "query_results"],"sparkSubmitParameters":"--class org.opensearch.flint.app.FlintREPL --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.jars=s3://flint-data-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar,s3://flint-data-dp-eu-west-1-beta/code/flint/opensearch-spark-standalone_2.12-0.1.0-SNAPSHOT.jar --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.datasource.flint.host='${osDomain}' --conf spark.datasource.flint.port=9200 --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}'
