package de.htwberlin.f4.wikiplag.utils

import de.htwberlin.f4.wikiplag.utils.properties.Properties
import org.apache.spark.SparkConf

/**
  * Utility class to easy the creation of a spark config.
  */
object SparkConfHelper {

  /**
    * Creates a cassandra SparkConf by setting the required parameters.
    * If the spark app is started within an IDE the spark master is also set to "local".
    **/
  def createCassandraSparkConf(appName: String, cassandraUser: String, cassandraPW: String, cassandraHost: String,
                               cassandraPort: String = "9042", loadDefaults: Boolean = true): SparkConf = {
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
}
