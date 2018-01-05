package de.htwberlin.f4.wikiplag.rest.servlets

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.plagiarism.{PlagiarismFinder, WikiExcerptBuilder}
import de.htwberlin.f4.wikiplag.rest.Text
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import org.apache.spark.SparkContext
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._

class WikiplagServlet extends ScalatraServlet with JacksonJsonSupport {

  protected implicit val jsonFormats: Formats = DefaultFormats

  private val separator: String = System.getProperty("file.separator")

  private var cassaandraClient: CassandraClient = _

  override def init(): Unit = {
    println("in init")

    var cassandraParameter = CassandraParameters.readFromConfigFile("app.conf")
    var conf = cassandraParameter.toSparkConf("[Wikiplag]REST-API")
    var sparkContext = new SparkContext(conf)

    cassaandraClient = new CassandraClient(sparkContext, cassandraParameter)
  }

  before() {}
  /*
	 * document path
	 */
  get("/documents/:id") {
    try {
      contentType = "text/plain"
      val wikiId = params("id").toInt
      println(s"get /documents/:id with $wikiId")
      val document = cassaandraClient.queryArticlesAsMap(List(wikiId))(wikiId).text
      val begin_index = document.indexOf("TEMPLATE")
      val end_index = document.indexOf("Weblinks")
      document.substring(begin_index, end_index)
    } catch {
      case e: StringIndexOutOfBoundsException => halt(404, "Not Found")
      case e: NumberFormatException => halt(400, "Invalid Id")
      case e: Exception =>
        e.printStackTrace()
        halt(500, "Internal server error")
    }
  }

  /*
  * plagiarism path
  */
  post("/analyse") {
    println("post /wikiplag/analyse")
    contentType = formats("json")
    try {
      // read json input file and convert to Text object
      val text_obj = parsedBody.extract[Text]
      val result = new PlagiarismFinder(cassaandraClient).findPlagiarisms(text_obj.text, new HyperParameters())
      val resultW = new WikiExcerptBuilder(cassaandraClient).buildWikiExcerpts(result, 3)

      resultW
    } catch {
      case e: org.json4s.MappingException => halt(400, "Malformed JSON")
      case e: Exception =>
        e.printStackTrace()
        halt(500, "Internal Server Error")
    }
  }
}
