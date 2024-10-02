package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object SQLJob extends Logging {
  def main(args: Array[String]) {
    if (args.length != 4) {
      logError("Usage: SparkQueryApp <track> <id><iterations> <query>")
      System.exit(1)
    }

    val track = args(0)
    val id = args(1)
    val iterations = Try(args(2).toInt).getOrElse {
      logError("Invalid number of iterations")
      System.exit(1)
      0
    }
    val query = args(3)
        // Initialize Spark session with AWS Glue Data Catalog as metastore
    val spark = SparkSession.builder()
      .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      )
      .enableHiveSupport()
      .getOrCreate()

    // warmup
    logInfo(s"Executing warmup query: $query")
    val startTime = System.nanoTime()
    try {
      spark.sql(query).show()
    } catch {
      case e: Exception =>
        logError(s"Error executing query: $query", e)
    }
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d // Convert to seconds
    logInfo(s"Finished warmup in $duration seconds")


    val futures = (1 to iterations).map { i =>
      Future {
        logInfo(s"Executing query iteration $i: $query")
        val startTime = System.nanoTime()

        try {
          spark.sql(query).show()
        } catch {
          case e: Exception =>
            logError(s"Error executing query iteration $i: $query", e)
        }

        val endTime = System.nanoTime()
        val duration = (endTime - startTime) / 1e9d
        logInfo(s"Finished iteration $i in $duration seconds")
        duration
      }
    }

    // Wait for all futures to complete and collect results
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    val latencies = Await.result(Future.sequence(futures), Duration.Inf)    

    // Calculate latencies: max, p50, and p75
    val sortedLatencies = latencies.sorted
    val maxLatency = sortedLatencies.last
    val p50Latency = sortedLatencies((0.5 * iterations).toInt)
    val p75Latency = sortedLatencies((0.75 * iterations).toInt)

    logInfo(f"$track-$id, max Latency: $maxLatency%.2f seconds")
    logInfo(f"$track-$id, p50 Latency: $p50Latency%.2f seconds")
    logInfo(f"$track-$id, p75 Latency: $p75Latency%.2f seconds")

    // Stop the Spark session
    spark.stop()
    System.exit(0)
  }
}
