package de.htwberlin.f4.wikiplag.utils

import de.htwberlin.f4.wikiplag.utils.properties.Properties
import org.apache.spark.SparkConf
import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.parsing.json.JSON

/**
  * Encapsulates cassandra parameters read from the command line or a file for easier handling.
  */
class CassandraParameters(val articlesTable: String, val inverseIndexTable: String, val tokenizedTable: String, val keyspace: String,
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
    if (Properties.runningFromIDE()) {
      println("running from IDE. Setting master to \"local\"")
      conf.setMaster("local")
    }
    conf
  }

  override def toString: String = s"CassandraParameters($articlesTable, $inverseIndexTable, $tokenizedTable, $keyspace," +
    s" $cassandraUser, $cassandraPW, $cassandraHost, $cassandraPort)"
}

object CassandraParameters {

  /** reads cassandra parameters from a file with the given path. Throws a ConfigException if something goes wrong.*/
  def readFromConfigFile(path: String): CassandraParameters = {

    var config = ConfigFactory.load(path)
    var user = config.getString("username")
    val password = config.getString("password")
    val host = config.getString("host")
    val port = config.getString("port")
    val articlesTable = config.getString("articles-table")
    val inverseIndexTable = config.getString("inverse-index-table")
    val tokenizedTable = config.getString("tokenized-table")
    val keyspace = config.getString("keyspace")

    new CassandraParameters(articlesTable, inverseIndexTable, tokenizedTable, keyspace, user, password, host, port)
  }

}
