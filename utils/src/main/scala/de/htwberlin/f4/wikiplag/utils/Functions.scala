package de.htwberlin.f4.wikiplag.utils

import scala.collection.mutable

/** Various utility functions
  */
object Functions {

  def SplitByMultipleIndices(indices: List[Int], s: String) = (indices zip indices.tail) map { case (a, b) => s.substring(a, b) }

  /*Returns the indices of all occurrences in s of the given keyword as an ordered list.*/
  def allIndicesOf(s: String, keyword: String): List[Int] = {
    var listBuffer = new mutable.ListBuffer[Int]
    var index = s.indexOf(keyword)
    while (index != -1) {
      listBuffer.append(index)
      index = s.indexOf(keyword, index + 1)
    }
    listBuffer.toList
  }
}
