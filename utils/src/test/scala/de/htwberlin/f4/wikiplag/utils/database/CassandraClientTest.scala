package de.htwberlin.f4.wikiplag.utils.database

import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.inverseindex.InverseIndexHashing
import org.apache.spark.SparkContext
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit


class CassandraClientTest extends AssertionsForJUnit {
  var client: CassandraClient = _
  var sc: SparkContext = _


  @Before def setUp() {
    var cassandraParameters = CassandraParameters.readFromConfigFile("app.conf")
    sc = new SparkContext(cassandraParameters.toSparkConf("Test CassandraClient"))
    client = new CassandraClient(sc, cassandraParameters)
  }

  @After def tearDown(): Unit = {
    sc.stop()
  }

  @Test def testQueryDocIdsTokens(): Unit ={
    val request = 1
    var result = client.queryDocIdsTokensAsMap(List(request))

    assert(result.nonEmpty)

    val alanSmitheeArticle=result(1)
    assert(alanSmitheeArticle.head=="alan")
    assert(alanSmitheeArticle(1)=="smithee")
  }


  @Test def testQueryNGramHashResultCorrectDocIdAndPosition() {
    val request = InverseIndexHashing.hash(List("blind", "guardian", "1984", "gegründete"))
    var result = client.queryNGramHashesAsArray(List(request))
    assert(result.nonEmpty)

    assert(result.head.getLong(InverseIndexTable.DocId)==50838)
    assert(result.head.get[List[Int]](InverseIndexTable.Occurrences).head==0)
  }

  @Test def testQueryNGramHashResultCorrectDocIdAndPosition2() {
    val request = InverseIndexHashing.hash(List("fiktiven", "regisseur", "filme", "verantwortet"))
    println(request)
    var result = client.queryNGramHashesAsArray(List(request))
    ////all except "und" ,the last n-1 words and the 3 duplicate entries count as 1
    assert(result.nonEmpty)

    assert(result.head.getLong(InverseIndexTable.DocId) == 1)
    assert(result.head.get[List[Int]](InverseIndexTable.Occurrences).head == 7)
  }
}