package de.htwberlin.f4.wikiplag.plagiarism.models

/**
  * Represents one potential plagiarism
  *
  * @param positon the text position of the plagiarism
  * @param docId   the docId id
  */
class Match(val positon: TextPosition, val docId: Int) {

  override def toString = s"Match(positon=$positon, docId=$docId)"
}