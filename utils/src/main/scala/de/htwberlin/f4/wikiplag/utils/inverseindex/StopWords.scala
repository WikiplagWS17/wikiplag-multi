package de.htwberlin.f4.wikiplag.utils.inverseindex

/**
  *
  * Reads stop words from a file and stores them in the [[StopWords.stopWords]] variable.
  *
  * Stop words taken from: https://solariz.de/de/deutsche_stopwords.htm
  */

object StopWords {
  val stopWords: Set[String] = Option(getClass.getClassLoader.getResourceAsStream("stopwords.txt"))
    .map(scala.io.Source.fromInputStream)
    .map(_.getLines.toSet)
    .getOrElse(scala.io.Source.fromFile("stopwords.txt").getLines.toSet)
}
