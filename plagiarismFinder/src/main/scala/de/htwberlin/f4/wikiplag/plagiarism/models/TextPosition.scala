package de.htwberlin.f4.wikiplag.plagiarism.models

/** Represents a position in a text
  *
  * @param start the start of the text
  * @param end   the end of the text position
  */
class TextPosition(var start: Int, var end: Int) {
  override def toString = s"TextPosition(start=$start, end=$end)"

  /**
    * Computes the length of the text position. The length is equal to start - end.
    */
  def length(): Int = {
    start - end
  }
}