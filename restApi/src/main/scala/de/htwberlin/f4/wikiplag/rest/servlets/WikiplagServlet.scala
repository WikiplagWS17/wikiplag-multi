package de.htwberlin.f4.wikiplag.rest.servlets

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.plagiarism.{PlagiarismFinder, WikiExcerptBuilder}
import de.htwberlin.f4.wikiplag.rest.Text
import de.htwberlin.f4.wikiplag.rest.models.RestApiPostResponseModel
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

  // for testing webapp
  private var saveJson: RestApiPostResponseModel = _

  override def init(): Unit = {
    println("in init")

    var cassandraParameter = CassandraParameters.readFromConfigFile("app.conf")
    var conf = cassandraParameter.toSparkConf("[Wikiplag]REST-API").setMaster("local")
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
   * for testing webapp
  */
  get("/test") {
    contentType = formats("json")
     saveJson
     }

  /*
  * plagiarism path
  */
  post("/analyse") {
    println("post /wikiplag/analyse")
    contentType = formats("json")
    try {
      // read json input file and convert to Text object
      val jsonString = request.body
      val jValue = parse(jsonString)
      val textObject = jValue.extract[Text]
      val plagiarism = new PlagiarismFinder(cassaandraClient).findPlagiarisms(textObject.text, new HyperParameters())
      val plagiarismExcrepts = new WikiExcerptBuilder(cassaandraClient).buildWikiExcerpts(plagiarism, 3)

      val result  = new RestApiPostResponseModel(plagiarismExcrepts)
      result.InitTaggedInputTextFromRawText(textObject.text)

      // for testing webapp
      saveJson = result

      result
    } catch {
      case e: org.json4s.MappingException => halt(400, "Malformed JSON")
      case e: Exception =>
        e.printStackTrace()
        halt(500, "Internal Server Error")
    }
  }
}
