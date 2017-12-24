package de.htwberlin.f4.wikiplag.utils

import de.htwberlin.f4.wikiplag.utils.properties.Properties
import org.apache.spark.SparkConf

/**
  * Encapsulates cassandra parameters read from the command line or a file for easier handling.
  */
class CassandraParameters(val articlesTable: String, val inverseIndexTable: String, val tokenizedTable: String, val keyspace:String,
                          val cassandraUser: String, val cassandraPW: String, val cassandraHost: String,
                          val cassandraPort: String = "9042") {

  /**
    * Creates a cassandra SparkConf from the parameters.
    * If the spark app is started within an IDE the spark master is also set to "local".
    **/
  def toSparkConf(appName: String, loadDefaults: Boolean = true): SparkConf = {
    val conf = new SparkConf(loadDefaults)
      .setAppName(appName)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)
      .set("spark.cassandra.auth.username", cassandraUser)
      .set("spark.cassandra.auth.password", cassandraPW)

    //specify master explicitly to local if started within the ide
    if (Properties.runningFromIDE())
      conf.setMaster("local")
    conf
  }

  override def toString = s"CassandraParameters($articlesTable, $inverseIndexTable, $tokenizedTable, $keyspace, $cassandraUser, $cassandraPW, $cassandraHost, $cassandraPort)"
}
