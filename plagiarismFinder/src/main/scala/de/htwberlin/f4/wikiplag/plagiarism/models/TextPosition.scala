package de.htwberlin.f4.wikiplag.plagiarism.models

/** Represents a position in a text
  *
  * @param start the start
  * @param end   the end of the text position */
class TextPosition(var start: Int, var end: Int) {
  override def toString = s"TextPosition(start=$start, end=$end)"

  /*start - end*/
  def length(): Int = {
    start - end
  }
}