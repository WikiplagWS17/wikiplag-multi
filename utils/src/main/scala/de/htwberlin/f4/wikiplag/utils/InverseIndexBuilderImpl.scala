package de.htwberlin.f4.wikiplag.utils

import java.util.Locale

import de.htwberlin.f4.wikiplag.utils.parser.WikiDumpParser

import scala.collection.mutable

/**
  * Created by chris on 06.11.2016.
  * Modified by Jörn Sattler on 25.07.2017
  * Modified by Anton K. 16.12.2017
  * Source: https://github.com/WikiPlag/wiki_data_fetcher
  *
  */
object InverseIndexBuilderImpl {

  var stopWords: Set[String] = _

  {
    loadStopWords()

    /**
      * Reads stop words from a file and stores them in the {@link InverseIndexBuilderImpl#stopWords} variable.
      *
      * @param stopWordsFile the file name.
      *                      Stopwords taken from: https://solariz.de/de/deutsche_stopwords.htm
      */
    def loadStopWords(stopWordsFile: String = "stopwords.txt") = {
      stopWords = Option(getClass.getClassLoader.getResourceAsStream(stopWordsFile))
        .map(scala.io.Source.fromInputStream)
        .map(_.getLines.toSet)
        .getOrElse(scala.io.Source.fromFile(stopWordsFile).getLines.toSet)
    }
  }


  /**
    * Removes stop words, builds n-grams and builds the inverse index for a certain document from the n-grams.
    *
    * There will be no normalization or stemming applied. You may use
    * [[InverseIndexBuilderImpl.tokenizeAndNormalize]] beforehand to achieve this.
    *
    * @param n      the n used for the n-grams
    * @param docId The document's identifier.
    * @param tokens The parsed and normalized words of the document.
    * @return The inverse index for the passed document (a map of n-gram hashes to document and occurrences tuples
    *
    */
  def buildInverseIndexNGramHashes(n: Int, docId: Int,
                                   tokens: List[String]): Map[Long, (Int, List[Int])] = {

    //we use a queue since all operations that we use are constant time on queues
    //http://docs.scala-lang.org/overviews/collections/performance-characteristics.html

    //we have to use -n+1 as our starting index because of the way our n-gram building method works.
    tokens.foldLeft(Map.empty[Long, (Int, List[Int])], mutable.Queue.empty[String], -n+1) {

      (accumulator, current) => {
        var resultNGramsToOccurrencesMap = accumulator._1
        val nNgramBuffer = accumulator._2
        val currentPosition = accumulator._3

        //if the current word s not a stop word add it to the n-gram buffer
        if (!stopWords.contains(current))
          nNgramBuffer.enqueue(current)

        //the are enough values to build an n-gram, let's do it!
        if (nNgramBuffer.size == n) {
          val ngram = nNgramBuffer.toList
          val ngramHash = InverseIndexHashing.hash(ngram)
          //remove the "key" (the first word) of the n-gram from the list
          nNgramBuffer.dequeue()

          //get the occurrences of the given n-gram. If not present return an empty lst
          val occurrencesList = resultNGramsToOccurrencesMap.getOrElse(ngramHash, (docId, List.empty[Int]))._2

          //add the new one to the list
          val newOccurrencesList = occurrencesList :+ currentPosition

          //update the change in the map
          resultNGramsToOccurrencesMap = resultNGramsToOccurrencesMap.updated(ngramHash, (docId, newOccurrencesList))
        }

        (resultNGramsToOccurrencesMap, nNgramBuffer, currentPosition + 1)
      }
    }._1
  }

  /**
    * Parses a given input text into a collection of words and transforms these words into tokens later used to build
    * an inverse index.
    * <p>
    * The used parser extracts symbols and characters of the wikipedia markup language from the input text. The text
    * will then be normalized.
    * </p>
    **
    * @param documentText the input text that should be parsed.
    * @return a collection of tokens already prepared to build an inverse index.
    */
  def tokenizeAndNormalize(documentText: String): List[String] = {
    var tokens = WikiDumpParser.extractWikiDisplayText(documentText)
    normalize(tokens)
  }

  /**
    * Normalizes the words. Currently only all lower case is applied.
    *
    * @param rawWords words that are not yet normalized
    * @return normalized tokens
    */
  def normalize(rawWords: List[String]): List[String] = {
    rawWords.map(x => x.toLowerCase(Locale.ROOT))

    /**
      * ToDo:
      * - apply stemming: only store root versions of words
      * - merge synonyms of words to one term
      *
      * The latter could really influence our similarity results in a bad way. We should take care that this is not
      * going in the wrong direction and suddenly everything becomes a plagiarism.
      *
      * For more information on how to build an inverse index, look up the elastic search doc.
      * see: https://www.elastic.co/guide/en/elasticsearch/guide/current/inverted-index.html
      */
  }
}
