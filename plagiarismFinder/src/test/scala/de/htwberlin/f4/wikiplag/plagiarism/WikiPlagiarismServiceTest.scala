package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.plagiarism.services.WikiPlagiarismService
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import org.apache.spark.SparkContext
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class WikiPlagiarismServiceTest extends AssertionsForJUnit {
  var n: Int = 4
  var finder: PlagiarismFinder = _
  var excerptBuilder: WikiPlagiarismService = _
  var sc: SparkContext = _

  @Before def setUp() {
    var cassandraParameters = CassandraParameters.readFromConfigFile("app.conf")

    val sparkConf = cassandraParameters.toSparkConf("[Wikiplag] Plagiarism finder")
    sc = new SparkContext(sparkConf)
    val cassandraClient = new CassandraClient(sc, cassandraParameters)
    finder = new PlagiarismFinder(cassandraClient)
    excerptBuilder = new WikiPlagiarismService(cassandraClient)
  }

  @After def tearDown() {
    sc.stop()
  }


  @Test def testExcerpt(): Unit = {
    val input = raw"Daniel Stenberg, der Programmierer von cURL, begann 1997 ein Programm zu schreiben, das IRC-Teilnehmern Daten über Wechselkurse zur Verfügung stellen sollte, welche von Webseiten abgerufen werden mussten. Er setzte dabei auf das vorhandene Open-Source-Tool httpget. Nach einer Erweiterung um andere Protokolle wurde das Programm am 20. März 1998 als cURL 4 erstmals veröffentlicht. Ursprünglich stand der Name für und wurde erst später vom Stenberg nach einem besseren Vorschlag zum aktuellen Backronym umgedeutet.[2] URL"
    val matches = finder.findPlagiarisms(input, new HyperParameters())
    val wikiExcerpt = excerptBuilder.createWikiPlagiarisms(matches, 15)

    println(wikiExcerpt.size)
    println(wikiExcerpt)
    println()


    wikiExcerpt.foreach(println(_))
    assert(true)
  }

  @Test def testExcerpt3(): Unit = {
    val input = raw"Korpiklaani (finn. „Klan der Wildnis“, auch „Klan des Waldes“) ist eine finnische Folk-Metal-Band aus Lahti mit starken Einflüssen aus der traditionellen Volksmusik. Die Texte der Band handeln von mythologischen Themen sowie der Natur und dem Feiern, wobei auch reine Instrumentalstücke in ihrem Repertoire enthalten sind. Sie selbst sehen ihre Musik auch vom Humppa beeinflusst. Bislang wurden sechs reguläre Studioalben und eine EP veröffentlicht, daneben eine Live-DVD, sowie eine Wiederveröffentlichung der Demos."
    val input2 = raw"Der Kragenbär, Asiatische Schwarzbär, Mondbär oder Tibetbär (Ursus thibetanus) ist eine Raubtierart aus der Familie der Bären (Ursidae). In seiner Heimat wird er meistens als black bear bezeichnet oder als Baribal. Im Vergleich zum eher gefürchteten Grizzlybär gilt der Schwarzbär als weniger gefährlich."

    //find plagiarisms using default hyper parameters
    val matches = finder.findPlagiarisms(input + input2, new HyperParameters())
    val wikiExcerpt = excerptBuilder.createWikiPlagiarisms(matches, 3)

    println(wikiExcerpt.size)
    println(wikiExcerpt)
    println()


    wikiExcerpt.foreach(println(_))
    assert(true)
  }

  @Test def testExcerpt2(): Unit = {
    val input = raw"Und der wwfawf werwe für einen fiktiven Regisseur der Filme verantwortet, bei denen der eigentliche Regisseur seinen Namen nicht Kontaktenfrom. zuletzt', weil Arthur  Hiller  der eigentliche  regisseur "
    val matches = finder.findPlagiarisms(input, new HyperParameters())
    val wikiExcerpt = excerptBuilder.createWikiPlagiarisms(matches, 3)

    println(wikiExcerpt.size)
    println(wikiExcerpt)
    println()


    wikiExcerpt.foreach(println(_))
    assert(true)
  }
}
