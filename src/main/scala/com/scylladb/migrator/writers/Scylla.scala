package com.scylladb.migrator.writers

import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{Credentials, Rename, TargetSettings}
import com.scylladb.migrator.readers.TimestampColumns
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Scylla {
  val log: Logger = LogManager.getLogger("com.scylladb.migrator.writer.Scylla")
  log.info("Scylla Target")
  def writeDataframe(
    target: TargetSettings.Scylla,
    renames: List[Rename],
    df: DataFrame,
    timestampColumns: Option[TimestampColumns],
    tokenRangeAccumulator: Option[TokenRangeAccumulator])(implicit spark: SparkSession): Unit = {
    val connector = Connectors.targetConnector(spark.sparkContext.getConf, target)
    val tempWriteConf = WriteConf
      .fromSparkConf(spark.sparkContext.getConf)

    val writeConf = {
      if (timestampColumns.nonEmpty) {
        tempWriteConf.copy(
          ttl = timestampColumns.map(_.ttl).fold(TTLOption.defaultValue)(TTLOption.perRow),
          timestamp = timestampColumns
            .map(_.writeTime)
            .fold(TimestampOption.defaultValue)(TimestampOption.perRow)
        )
      } else if (spark.conf.get("spark.target.writeTTLInS").nonEmpty || spark.conf.get("spark.target.writeWritetimestampInuS").nonEmpty) {
        var hardcodedTempWriteConf = tempWriteConf
        if (spark.conf.get("spark.target.writeTTLInS").nonEmpty) {
          hardcodedTempWriteConf =
            hardcodedTempWriteConf.copy(ttl = TTLOption.constant(Option(spark.conf.get("spark.target.writeTTLInS")).map(_.toInt).get))
        }
        if (spark.conf.get("spark.target.writeWritetimestampInuS").nonEmpty) {
          hardcodedTempWriteConf = hardcodedTempWriteConf.copy(
            timestamp = TimestampOption.constant(Option(spark.conf.get("spark.target.writeWritetimestampInuS")).map(_.toLong).get))
        }
        hardcodedTempWriteConf
      } else {
        tempWriteConf
      }
    }

    // Similarly to createDataFrame, when using withColumnRenamed, Spark tries
    // to re-encode the dataset. Instead we just use the modified schema from this
    // DataFrame; the access to the rows is positional anyway and the field names
    // are only used to construct the columns part of the INSERT statement.
    val renamedSchema = renames
      .foldLeft(df) {
        case (acc, Rename(from, to)) => acc.withColumnRenamed(from, to)
      }
      .schema

    log.info("Schema after renames:")
    log.info(renamedSchema.treeString)

    // target.credentials match {
    //   case Some(Credentials(username, password)) => {        
    //     df.write
    //     .format("org.apache.spark.sql.cassandra")
    //     .options(Map(
    //     "keyspace" -> target.keyspace,
    //     "table" -> target.table,
    //     "spark.files" -> "/app/secure-connect-database.zip",
    //     "spark.cassandra.connection.config.cloud.path" -> "secure-connect-database.zip",
    //     "spark.cassandra.auth.username" -> username,
    //     "spark.cassandra.auth.password" -> password
    //     )).save()
    //   }
    // }

    val keyspace = spark.conf.get("spark.target.keyspace")
    val tableName = spark.conf.get("spark.target.table")
    
    val columnSelector = SomeColumns(renamedSchema.fields.map(_.name: ColumnRef): _*)
    // Spark's conversion from its internal Decimal type to java.math.BigDecimal
    // pads the resulting value with trailing zeros corresponding to the scale of the
    // Decimal type. Some users don't like this so we conditionally strip those.

    val rdd =
      if (!target.stripTrailingZerosForDecimals) df.rdd
      else
        df.rdd.map { row =>
          Row.fromSeq(row.toSeq.map {
            case x: java.math.BigDecimal => x.stripTrailingZeros()
            case x                       => x
          })
        }

    rdd
      .saveToCassandra(
        keyspace,
        tableName,
        columnSelector,
        writeConf ,
        tokenRangeAccumulator = tokenRangeAccumulator
      )(connector, SqlRowWriter.Factory)
  }

}
