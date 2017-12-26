package de.htwberlin.f4.wikiplag.utils.database

import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable
import de.htwberlin.f4.wikiplag.utils.{CassandraParameters, InverseIndexBuilderImpl, InverseIndexHashing}
import org.apache.spark.SparkContext
import org.junit.{After, Before, BeforeClass, Test}
import org.scalatest.junit.AssertionsForJUnit


class CassandraClientTest extends AssertionsForJUnit() {
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

  @Test def testQueryNGramHashResultNonEmpty() {
    val request = InverseIndexHashing.hash(List("blind", "guardian", "1984", "gegründete"))
    var result = client.queryNGramHashesAsArray(List(request))
    assert(result.nonEmpty)

    assert(result.head.getLong(InverseIndexTable.DocId) == 50838)
    assert(result.head.get[List[Int]](InverseIndexTable.Occurrences).head == 0)
  }
}