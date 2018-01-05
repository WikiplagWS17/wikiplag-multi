package de.htwberlin.f4.wikiplag.rest

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatra._
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import de.htwberlin.f4.wikiplag.plagiarism.{PlagiarismFinder, HyperParameters}
import de.htwberlin.f4.wikiplag.utils.CassandraParameters

class WikiplagServlet extends ScalatraServlet with JacksonJsonSupport {


  protected implicit val jsonFormats: Formats = DefaultFormats

  private val separator: String = System.getProperty("file.separator")
  private var sparkContext: SparkContext = _
  private var cassandraParameter: CassandraParameters = _

  private var keyspace: String = _

  private var table: String = _

  override def init(): Unit = {
    println("in init")

    cassandraParameter = CassandraParameters.readFromConfigFile("app.conf")
    var conf = cassandraParameter.toSparkConf("[Wikiplag]REST-API")
    sparkContext = new SparkContext(conf)

    keyspace = cassandraParameter.keyspace
    table = cassandraParameter.articlesTable
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
      document.substring(begin_index, end_index)
    } catch {
      case e: StringIndexOutOfBoundsException => halt(404, "Not Found")
    }
  }


  /*
  * plagiarism path
  */
  post("/wikiplag/analyse") {
    println("post /wikiplag/analyse")
    contentType = "text/plain"
    // read json input file and convert to Text object
    try {
      val text_obj = parsedBody.extract[Text]
      val result = new PlagiarismFinder(this.sparkContext, this.cassandraParameter).findPlagiarisms(text_obj.text, new HyperParameters())
      result
    }catch {
      case e:org.json4s.MappingException=> halt(400,"Malformed JSON")
    }

  }

}
