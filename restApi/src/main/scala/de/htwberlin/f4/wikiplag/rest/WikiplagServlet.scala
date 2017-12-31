package de.htwberlin.f4.wikiplag.rest

import com.datastax.spark.connector.TupleValue
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatra._
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Formats}


class WikiplagServlet extends ScalatraServlet {


  protected implicit val jsonFormats: Formats = DefaultFormats

  private val separator: String = System.getProperty("file.separator")
  private var sparkContext: SparkContext = _
  //private val documentCache = mutable.Map[Long, Document]()
  private val SlidingSize = 30

  private var keyspace: String = _

  private var table: String = _

  override def init(): Unit = {
    println("in init")
    var config = ConfigFactory.load("backend.properties")

    val conf = new SparkConf(true)
      .setMaster(config.getString("spark.master"))
      .setAppName("WikiplagBackend")
      .set("spark.executor.memory", "4g")
      .set("spark.storage.memoryFraction", "0.8")
      .set("spark.driver.memory", "2g")
      .set("spark.cassandra.connection.host", config.getString("cass.host"))
      .set("spark.cassandra.connection.port", config.getString("cass.port"))
      .set("spark.cassandra.auth.username", config.getString("cass.user"))
      .set("spark.cassandra.auth.password", config.getString("cass.password"))

    sparkContext = new SparkContext(conf)

    keyspace = config.getString("cass.keyspace")

    table = config.getString("cass.wikiTable")
  }

  before() {}

  get("/") {
   "Would Elijah Wood? If Elijah Wood could Elijah Wood would"
  }

  /*
	 * document path
	 */
  get("/wikiplag/document/:id") {
    contentType = "text/plain"
    val wikiId = params("id").toInt
    println(s"get /wikiplag/document/:id with $wikiId")
    //get cassandra wikipedia originals table from config data
    val df = sparkContext.cassandraTable(this.keyspace, this.table)
    val document = df.select("wikitext")
                     .where("docid = ?", wikiId)
                     .collect()
                     .toList
                     .toString()

    try {
      val begin_index = document.indexOf("TEMPLATE")
      val end_index = document.indexOf("Weblinks")
      document.substring(begin_index,end_index)
    }catch{
      case e:StringIndexOutOfBoundsException => halt(404,"Not Found")
    }
  }


}
