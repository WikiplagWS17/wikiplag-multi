package de.htw.ai.Wiki_Importer

import de.htw.ai.Wiki_Importer.utils.InverseIndexBuilderImpl
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner



/**
  * Created by chris on 06.11.2016.
  */
@RunWith(classOf[JUnitRunner])
class InverseIndexTest extends FunSuite {

  trait TestObject {
    val testObject = InverseIndexBuilderImpl
    //testObject.loadStopWords()
  }

  test("testEmptyList") {


    new TestObject {
      val map_e1 = testObject.tokenizeAndNormalize("")
      assert(map_e1.isEmpty)
    }
  }

  test("testBuildInverseIndexEntry(doc_id, pageWordsAsList)") {

    val doc_id = 13
    val temsInDocument1 = List("Ä", "Ü", "Ö", "Ελλάδα", "Elláda","und", "Ä","und", "Ü", "Ö", "Ελλάδα", "Elláda")

    new TestObject {
      val map_e1: Map[List[String], (Long, List[Int])] = testObject.buildInverseIndexNGram(1,doc_id, temsInDocument1)

      assert(map_e1.size == 5)
      assert(map_e1(List("Ä"))._1 == doc_id)
      assert(map_e1(List("Ä"))._2.size == 2)
      assert(map_e1(List("Ä"))._2(1) == 6)
      assert(map_e1(List("Ä"))._2(0) == 0)
    }
  }
}