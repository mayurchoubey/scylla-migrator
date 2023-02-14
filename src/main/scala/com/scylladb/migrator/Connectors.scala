package com.scylladb.migrator

import java.net.{ InetAddress, InetSocketAddress }

import org.apache.log4j.{ Level, LogManager, Logger }

import com.datastax.spark.connector.cql.{
  CassandraConnector,
  CassandraConnectorConf,
  IpBasedContactInfo,
  NoAuthConf,
  PasswordAuthConf
}
import com.scylladb.migrator.config.{ Credentials, SourceSettings, TargetSettings }
import org.apache.spark.SparkConf

object Connectors {
  val log = LogManager.getLogger("com.scylladb.migrator.Connectors")
  def sourceConnector(sparkConf: SparkConf, sourceSettings: SourceSettings.Cassandra) = {

    var sourceIps = (sparkConf.get("spark.source.host") + "").split(',')
    var sourceAddresses: Set[InetSocketAddress] = Set()
    for (sourceIp <- sourceIps) {
      sourceAddresses += new InetSocketAddress(sourceIp, sparkConf.get("spark.source.port").toInt )
    }
    log.info("*** sourceAddresses *** " + sourceAddresses)

    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = sourceAddresses,
          authConf = PasswordAuthConf(sparkConf.get("spark.source.username") , sparkConf.get("spark.source.password") ),
          cassandraSSLConf = sourceSettings.sslOptions match {
            case None => CassandraConnectorConf.DefaultCassandraSSLConf
            case Some(sslOptions) =>
              CassandraConnectorConf.CassandraSSLConf(
                enabled            = sslOptions.enabled,
                trustStorePath     = sslOptions.trustStorePath,
                trustStorePassword = sslOptions.trustStorePassword,
                trustStoreType     = sslOptions.trustStoreType.getOrElse("JKS"),
                protocol           = sslOptions.protocol.getOrElse("TLS"),
                enabledAlgorithms = sslOptions.enabledAlgorithms.getOrElse(
                  Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")),
                clientAuthEnabled = sslOptions.clientAuthEnabled,
                keyStorePath      = sslOptions.keyStorePath,
                keyStorePassword  = sslOptions.keyStorePassword,
                keyStoreType      = sslOptions.keyStoreType.getOrElse("JKS")
              )
          }
        ),
        localDC                      = sourceSettings.localDC,
        localConnectionsPerExecutor  = Option(sparkConf.get("spark.source.connections")).map(_.toInt),
        remoteConnectionsPerExecutor = Option(sparkConf.get("spark.source.connections")).map(_.toInt),
        queryRetryCount              = -1
      )
    )
  }

  def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) = {

    var targetIps = (targetSettings.host + "").split(',')
    var targetAddresses: Set[InetSocketAddress] = Set()
    for (targetIp <- targetIps) {
      targetAddresses += new InetSocketAddress(targetIp, targetSettings.port)
    }
    log.info("*** targetAddresses *** " + targetAddresses)

    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = targetAddresses,
          authConf = targetSettings.credentials match {
            case None                                  => NoAuthConf
            case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
          },
          cassandraSSLConf = targetSettings.sslOptions match {
            case None => CassandraConnectorConf.DefaultCassandraSSLConf
            case Some(sslOptions) =>
              CassandraConnectorConf.CassandraSSLConf(
                enabled            = sslOptions.enabled,
                clientAuthEnabled  = sslOptions.clientAuthEnabled,
                trustStorePath     = sslOptions.trustStorePath,
                trustStorePassword = sslOptions.trustStorePassword,
                trustStoreType     = sslOptions.trustStoreType.getOrElse("JKS"),
                protocol           = sslOptions.protocol.getOrElse("TLS"),
                keyStorePath       = sslOptions.keyStorePath,
                keyStorePassword   = sslOptions.keyStorePassword,
                enabledAlgorithms = sslOptions.enabledAlgorithms.getOrElse(
                  Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")),
                keyStoreType = sslOptions.keyStoreType.getOrElse("JKS")
              )
          }
        ),
        localDC                      = targetSettings.localDC,
        localConnectionsPerExecutor  = Option(sparkConf.get("spark.target.connections")).map(_.toInt),
        remoteConnectionsPerExecutor = Option(sparkConf.get("spark.target.connections")).map(_.toInt),
        queryRetryCount              = -1
      )
    )
  }

  // def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) =
  //   new CassandraConnector(
  //     CassandraConnectorConf(sparkConf)
  //   )

  def targetConnectorAstra(sparkConf: SparkConf, targetSettings: TargetSettings.Astra) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        localConnectionsPerExecutor  = Option(sparkConf.get("spark.target.connections")).map(_.toInt),
        remoteConnectionsPerExecutor = Option(sparkConf.get("spark.target.connections")).map(_.toInt),
        queryRetryCount              = -1
      )
    )
}
