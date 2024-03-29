#!/bin/zsh

sql="select * from default.http_logs limit 1"

applicationId=00fd777k3k3ls20p

executionRole=arn:aws:iam::270824043731:role/emr-job-execution-role
role=arn:aws:iam::924196221507:role/FlintOpensearchServiceRole

osDomain=search-managed-flint-os-1-yptv4jzmlqwmltxje42bplwj2a.eu-west-1.es.amazonaws.com

aws emr-serverless start-job-run \
  --region eu-west-1 \
  --application-id ${applicationId} \
  --execution-role-arn ${executionRole}  \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://flint-code-service-account-dp-eu-west-1-beta/code/flint/sql-dynamic-job.jar","entryPointArguments":["'${sql}'", "query_results", "spark.datasource.flint.host='${osDomain}',spark.datasource.flint.port=-1,spark.datasource.flint.scheme=https,spark.datasource.flint.auth=sigv4,spark.datasource.flint.region=eu-west-1,spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider"],"sparkSubmitParameters":"--class org.opensearch.sql.FlintDynamicConfigJob --conf spark.jars=s3://flint-code-service-account-dp-eu-west-1-beta/code/flint/AWSGlueDataCatalogHiveMetaStoreAuth-1.0.jar --conf spark.jars.packages=org.opensearch:opensearch-spark-standalone_2.12:0.1.0-SNAPSHOT --conf spark.jars.repositories=https://aws.oss.sonatype.org/content/repositories/snapshots --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64 --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.hive.metastore.glue.role.arn='${role}' --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}' --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN='${role}'"}}'



