package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import org.apache.spark.SparkContext
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class WikiExcerptBuilderTest extends AssertionsForJUnit {
  var n: Int = 4
  var finder: PlagiarismFinder = _
  var excerptBuilder: WikiExcerptBuilder=_
  var sc: SparkContext = _

  @Before def setUp() {
    var cassandraParameters = CassandraParameters.readFromConfigFile("app.conf")

    val sparkConf = cassandraParameters.toSparkConf("[Wikiplag] Plagiarism finder")
    sc = new SparkContext(sparkConf)
    val cassandraClient = new CassandraClient(sc, cassandraParameters)
    finder = new PlagiarismFinder(cassandraClient)
    excerptBuilder=new WikiExcerptBuilder(cassandraClient)
  }

  @After def tearDown() {
    sc.stop()
  }


  @Test def testExcerpt(): Unit ={
    val input = raw"Daniel Stenberg, der Programmierer von cURL, begann 1997 ein Programm zu schreiben, das IRC-Teilnehmern Daten über Wechselkurse zur Verfügung stellen sollte, welche von Webseiten abgerufen werden mussten. Er setzte dabei auf das vorhandene Open-Source-Tool httpget. Nach einer Erweiterung um andere Protokolle wurde das Programm am 20. März 1998 als cURL 4 erstmals veröffentlicht. Ursprünglich stand der Name für und wurde erst später vom Stenberg nach einem besseren Vorschlag zum aktuellen Backronym umgedeutet.[2] URL"
    val matches = finder.findPlagiarisms(input, new HyperParameters())
    //TODO THROWS EXCEPTION
    val wikiExcerpt=excerptBuilder.buildWikiExcerpts(matches,3)

    println(wikiExcerpt.size)
    println(wikiExcerpt)
    println()


    wikiExcerpt.foreach(println(_))
    assert(true)
  }
}
