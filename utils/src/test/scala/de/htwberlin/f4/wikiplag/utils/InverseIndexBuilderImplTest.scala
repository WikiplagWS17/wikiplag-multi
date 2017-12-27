package de.htwberlin.f4.wikiplag.utils

import de.htwberlin.f4.wikiplag.utils.inverseindex.{InverseIndexBuilderImpl, InverseIndexHashing}
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class InverseIndexBuilderImplTest extends AssertionsForJUnit {
  var n: Int = 4
  var document: List[String] = _
  var result: Map[Long, (Int, List[Int])] = _
  var docId: Int= _

  @Before def init() {
    docId = 13
    document = List("Who", "would", "cross", "the", "Bridge", "of", "Death", "must", "answer", "me", "these", "questions", "three",
      "und", "before", "must", "answer", "me", "these", "must", "answer", "me", "these")
    result = InverseIndexBuilderImpl.buildInverseIndexNGramHashes(n, docId, document)
  }

  @Test def testBuildInverseHashCorrectNumberOfHashes() {
    //all except "und" ,the last n-1 words and the 3 duplicate entries count as 1
    assert(document.size - (1 + n - 1 + 2) == result.size)
  }

  @Test def testBuildInverseHashCorrectLength() {
    val totalNgrams = result.map(x => x._2._2.size).sum
    //all except "und" and the last n-1 words
    assert(document.size - (1 + n - 1) == totalNgrams)
  }

  @Test def testBuildInverseHashCorrectDocId() {
    assert(result.head._2._1 == docId)
  }

  @Test def testBuildInverseHashPositionsStartingAtZero() {
    val min_position = result.map(x => x._2._2.min).min
    assert(min_position == 0)
  }

  @Test def testBuildInverseHashCorrectHashValue() {
    val expectedHash = InverseIndexHashing.hash(List("Who", "would", "cross", "the"))
    assert(result(expectedHash)._2.head == 0)
  }

  @Test def testBuildInverseHashPositionsCorrect() {
    val ngramHash= InverseIndexHashing.hash(List("would", "cross", "the","Bridge"))
    assert(result(ngramHash)._2.head== 1)
  }
  @Test def testBuildInverseHashPositionsCorrectAfterStopword() {
    val ngramHash = InverseIndexHashing.hash(List("before", "must", "answer", "me"))
    assert(result(ngramHash)._2.head == 14)
  }

    @Test def testBuildInverseHashCombineDuplicateKeys() {
      val ngramHash= InverseIndexHashing.hash(List("must", "answer","me","these"))
      assert(result(ngramHash)._2.size== 3)
    }

  @Test def testBuildInverseHashPositionsCorrectBeforeStopword() {
    val ngramHash = InverseIndexHashing.hash(List("me", "these", "questions", "three"))
    assert(result(ngramHash)._2.head == 9)
  }

  @Test def testBuildInverseHashPositionsCorrectSplitByStopword() {
    val ngramHash = InverseIndexHashing.hash(List("three","before", "must", "answer"))
    assert(result(ngramHash)._2.head == 12)
  }
}