package de.htw.ai.Wiki_Importer.plagiarism

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner

//TODO
@RunWith(classOf[JUnitRunner])
class PlagiarismFinderTest extends FunSuite with BeforeAndAfterAll{

   var sc: SparkContext = _
   var sparkConf: SparkConf = _
   var cassandrahost = "<Host>"
   var cassandraport = "<Port>"
   var cassandrausername = "<Username>"
   var cassandrapw = "<Password>"
   var cassandrawikitab = "<Wikitable_Name>"
   var cassandrainvIndextab = "<Iverse Indezies Table Name>"
   var cassandrakeyspace = "<Keyspace Name>"
  
   override protected def beforeAll() { 
      sparkConf = new SparkConf(true).setAppName("Tester")
      .set("spark.cassandra.connection.host", s"${cassandrahost}")
      .set("spark.cassandra.connection.port", s"${cassandraport}")
      .set("spark.cassandra.auth.username", s"${cassandrausername}")
      .set("spark.cassandra.auth.password", s"${cassandrapw}")
    sc = new SparkContext(sparkConf)
   }

  
  test("findexacttitle_Cassandra") {
    val df = sc.cassandraTable(cassandrakeyspace, cassandrawikitab)
    val article = df.select("docid","title", "wikitext").where("title = ?", "Anime") 
    assert(article.count() == 1)
    assert(article.first().getString(1) == "Anime")
  }
  
  test("finddocID_Cassandra") {
    val df = sc.cassandraTable(cassandrakeyspace, cassandrawikitab)
    val articles = df.select("docid","title", "wikitext").where("docid = ?", "1")   
    assert(articles.first().getLong(0) == 1)  
  }

    test("finddocwords_cassandra") {
    val df = sc.cassandraTable(cassandrakeyspace, cassandrainvIndextab)
    val articles = df.select("word","docid", "occurences").where("docid = ?", "1")
    assert(articles.count() == 282)
    assert(articles.first().getString(0) == "aran")
    
  }
  
   test("findworddocs_cassandra") {
    val df = sc.cassandraTable(cassandrakeyspace, cassandrainvIndextab)
    val articles = df.select("word","docid", "occurences").where("word = ?", "anime")
    assert(articles.first().getString(0) == "anime")
    
  }  
  
  override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
   }
  
}
