package de.htwberlin.f4.wikiplag.rest

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatra._
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import de.htwberlin.f4.wikiplag.plagiarism.{PlagiarismFinder,HyperParameters}
import de.htwberlin.f4.wikiplag.utils.CassandraParameters

class WikiplagServlet extends ScalatraServlet with JacksonJsonSupport {


  protected implicit val jsonFormats: Formats = DefaultFormats

  private val separator: String = System.getProperty("file.separator")
  private var sparkContext: SparkContext = _
  private var cassandra_parameter:CassandraParameters = _
  private val SlidingSize = 30

  private var keyspace: String = _

  private var table: String = _

  override def init(): Unit = {
    println("in init")
    var config = ConfigFactory.load("backend.properties")

    val conf = new SparkConf(true)
      .setMaster(config.getString("spark.master"))
      .setAppName("WikiplagBackend")
      .set("spark.cassandra.connection.host", config.getString("cass.host"))
      .set("spark.cassandra.connection.port", config.getString("cass.port"))
      .set("spark.cassandra.auth.username", config.getString("cass.user"))
      .set("spark.cassandra.auth.password", config.getString("cass.password"))

    sparkContext = new SparkContext(conf)

    keyspace = config.getString("cass.keyspace")
    table = config.getString("cass.wikiTable_ar")

    cassandra_parameter = new CassandraParameters(config.getString("cass.wikiTable_ar"),
                                                  config.getString("cass.wikiTable_inv"),
                                                  config.getString("cass.wikiTable_to"),
                                                  config.getString("cass.keyspace"),
                                                  config.getString("cass.user"),
                                                  config.getString("cass.password"),
                                                  config.getString("cass.host"))
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


    /*
    * plagiarism path
    */
  post("/wikiplag/analyse") {
    println("post /wikiplag/analyse")
    contentType = "text/plain"
    // read json input file and convert to Text object
    val text_obj = parsedBody.extract[Text]


    val result = new PlagiarismFinder(this.sparkContext,this.cassandra_parameter)
                                      .findPlagiarisms(text_obj.text,new HyperParameters())

    println(result.toList.toString())
   "success"
  }

}
