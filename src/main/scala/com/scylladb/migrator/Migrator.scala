package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths, StandardOpenOption, Path}
import java.nio.channels.WritableByteChannel
import java.nio.ByteBuffer
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.config._
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import sun.misc.{ Signal, SignalHandler }
import org.apache.spark.{SparkContext, SparkConf, SparkFiles}
import sys.process._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;

import scala.util.control.NonFatal

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .getOrCreate
    
    val tableName = spark.conf.get("spark.source.table")
    //val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

    log.info("**** Migration Begins ****")

    var configFileContent = "";
    var migratorConfig = MigratorConfig(null,null,null,null,null,null);
    if(spark.conf.get("spark.scylla.deployment").equals("dataproc")){
      val configRdd = spark.sparkContext.wholeTextFiles(spark.conf.get("spark.scylla.config"))
      configRdd.collect.foreach(t=>configFileContent = t._2)
      migratorConfig = MigratorConfig.loadFromFileContent(configFileContent)
    } else{
      migratorConfig = MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))
    }
    
    log.info(s"Loaded config: ${migratorConfig}")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    val sourceDF =
      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          readers.Cassandra.readDataframe(
            spark,
            cassandraSource,
            spark.conf.get("spark.source.splitCount").toInt,
            spark.conf.get("spark.source.preserveTimestamps").toBoolean,
            migratorConfig.skipTokenRanges)
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    val tokenRangeAccumulator =
      if (!sourceDF.savepointsSupported) None
      else {
        val tokenRangeAccumulator = TokenRangeAccumulator.empty
        spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

        addUSR2Handler(migratorConfig, tokenRangeAccumulator, tableName, spark)
        startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator, tableName, spark)

        Some(tokenRangeAccumulator)
      }

    log.info(
      "We need to transfer: " + sourceDF.dataFrame.rdd.getNumPartitions + " partitions in total")

    if (migratorConfig.source.isInstanceOf[SourceSettings.Cassandra]) {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => {
        p.asInstanceOf[CassandraPartition[_, _]]
      })
      var allTokenRanges = Set[(Token[_], Token[_])]()
      cassandraPartitions.foreach(p => {
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach(tr => {
            val range =
              Set((tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]]))
            allTokenRanges = allTokenRanges ++ range
          })

      })

      log.info("All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (migratorConfig.skipTokenRanges != None) {
        log.info(
          "Savepoints array defined, size of the array: " + migratorConfig.skipTokenRanges.size)

        val diff = allTokenRanges.diff(migratorConfig.skipTokenRanges)
        log.info("Diff ... total diff of full ranges to savepoints is: " + diff.size)
        log.debug("Dump of the missing tokens: ")
        log.debug(diff)
      }
    }

    log.info("Starting write...")

    try {
      migratorConfig.target match {
        case target: TargetSettings.Scylla =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            tokenRangeAccumulator)
        case target: TargetSettings.Astra =>
          writers.Astra.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            tokenRangeAccumulator)
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
        throw new Exception("Exception thrown from Scylla Migrator");
    } finally {
      tokenRangeAccumulator.foreach(dumpAccumulatorState(migratorConfig, _, "final",tableName,spark))
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String, tableName: String): String =
    s"${path}/savepoint_${tableName}.yaml"

  def dumpAccumulatorState(config: MigratorConfig,
                           accumulator: TokenRangeAccumulator,
                           reason: String,
                           tableName: String,
                           spark: SparkSession
                           ): Unit = {

    val savepointDirPath = config.savepoints.path + "/" + tableName + "/"
    val createDirCommand = "mkdir -p " + savepointDirPath
    val result = createDirCommand !
    val fileCompletePath = savepointFilename(savepointDirPath, tableName)
    val fileAbsoluteName = Paths.get(fileCompletePath).normalize

    val rangesToSkip = accumulator.value.get.map(range =>
      (range.range.start.asInstanceOf[Token[_]], range.range.end.asInstanceOf[Token[_]]))

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges ++ rangesToSkip
    )

    Files.write(fileAbsoluteName, modifiedConfig.render.getBytes(StandardCharsets.UTF_8),
         StandardOpenOption.CREATE,
         StandardOpenOption.TRUNCATE_EXISTING)
    
    log.info(
      s"Created a savepoint config at ${fileAbsoluteName} due to ${reason}. Ranges added: ${rangesToSkip}")
      
    val bucketName : String = spark.conf.get("spark.savepoint.bucketname")
    var objectName = s"savepoint_${tableName}.yaml"

    try {
      val objectFolderName = spark.conf.get("spark.savepoint.objectFolder") + s"/savepoint_${tableName}.yaml"
      objectName = objectFolderName
    } catch{
      case e: Throwable =>
      log.info("No savepoint object folder provided hence creating savepoint file inside bucket " + bucketName + " directly.")
    }

    val storage : Storage = StorageOptions.getDefaultInstance().getService()

    val bucket : Bucket = storage.get(bucketName)
    if(bucket != null && bucket.exists()){
      val blobExisting : Blob = storage.get(bucketName,objectName)
        if (blobExisting != null && blobExisting.exists()){
          val channel : WritableByteChannel = blobExisting.writer()
          channel.write(ByteBuffer.wrap(MigratorConfig.loadConfigString(fileCompletePath).getBytes(StandardCharsets.UTF_8)))
          channel.close()
        } else {
          val blobId : BlobId = BlobId.of(bucketName, objectName);
          val blobInfo : BlobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
          val blob : Blob = storage.create(blobInfo, MigratorConfig.loadConfigString(fileCompletePath).getBytes(StandardCharsets.UTF_8));
        }
    } else {
      throw new Exception("Savepoint bucket " + bucketName + " does not exist.")
    }               
    
    log.info(
      s"Created a savepoint config at GCP due to ${reason}. Ranges added: ${rangesToSkip}")
  }

  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor,
                             config: MigratorConfig,
                             acc: TokenRangeAccumulator,
                             tableName: String,
                             spark: SparkSession): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpAccumulatorState(config, acc, "schedule", tableName, spark)
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")

    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
  }

  def addUSR2Handler(config: MigratorConfig, acc: TokenRangeAccumulator, tableName: String, spark: SparkSession) = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpAccumulatorState(config, acc, signal.toString, tableName, spark)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
  }
}
