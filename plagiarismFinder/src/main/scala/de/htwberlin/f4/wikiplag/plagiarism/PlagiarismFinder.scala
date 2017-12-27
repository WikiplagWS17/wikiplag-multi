package de.htwberlin.f4.wikiplag.plagiarism

import com.datastax.spark.connector.CassandraRow
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable
import de.htwberlin.f4.wikiplag.utils.inverseindex.{InverseIndexBuilderImpl, InverseIndexHashing, StopWords}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Represents one potential plagiarism
  *
  * @param positon the text position of the plagiarism
  * @param docId   the docId id
  */
class Match(val positon: TextPosition, val docId: Int) {

  override def toString = s"Match(positon=$positon, docId=$docId)"
}

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

  def +(other: TextPosition): TextPosition = {
    new TextPosition(this.start, this.end + other.start)
  }
}

/**
  * @author
  * Anton K.
  *
  * Encapsulates the hyper parameters.
  * @param minimumSentenceLength    The minimum length (number of words) in a sentence.
  *                                 Shorter sentences are merged together to be atleast this long.
  * @param threshold                The initial threshold.
  *                                 Articles which have an unique n-grams to input n-grams ratio
  *                                 below this threshold are filtered out
  * @param maxDistanceBetweenNgrams The maximum distance between two n-grams.
  *                                 If the distance of 2 given n-grams is above this
  *                                 they are split into separate plagiarism segments
  * @param maxAverageDistance       The maximum average distance between a potential plagiarism segment.
  * @param secondaryThreshold       The secondary threshold - applied after building plagiarism segments
  *                                 similarly to [[HyperParameters.threshold]]
  **/
class HyperParameters(val minimumSentenceLength: Int = 6, val threshold: Float = 0.85f,
                      val maxDistanceBetweenNgrams: Int = 6, val maxAverageDistance: Int = 3,
                      val secondaryThreshold: Float = 0.80f) {

  override def toString: String = s"HyperParameters(minimumSentenceLength=$minimumSentenceLength," +
    s" threshold=$threshold, maxDistanceBetweenNgrams=$maxDistanceBetweenNgrams," +
    s" maxAverageDistance=$maxAverageDistance, secondaryThreshold=$secondaryThreshold)"
}

/**
  * @author
  * Anton K.
  *
  * 27.12.2017
  *
  * A class for determining if a given text has parts which are potential wikipedia plagiarisms.
  * @constructor Creates a new instance.
  * @param cassandraParameters cassandra parameters
  * @param sc                  the spark context
  **/
class PlagiarismFinder(sc: SparkContext, cassandraParameters: CassandraParameters, n: Int = 4) {

  val cassandraClient: CassandraClient = new CassandraClient(sc, cassandraParameters)

  /** */
  def findPlagiarismsExtendedText(rawText: String, hyperParameters: HyperParameters): Map[TextPosition, List[String]] = {
    var potentialPlagiarims = findPlagiarisms(rawText, hyperParameters)

    val docIds = potentialPlagiarims.flatMap(x => x._2).map(x => x.docId).toList

    val docIdsToDocsMap = cassandraClient.queryDocIdsTokensAsMap(docIds)

    val result = potentialPlagiarims.map(textPlagiarismPair => textPlagiarismPair._1 ->
      textPlagiarismPair._2.map(m => expandToIncludeStopWordsAsText(m, docIdsToDocsMap(m.docId), hyperParameters.maxDistanceBetweenNgrams)))

    result
  }

  /**
    * Returns potential plagiarism matches to the given raw text input.
    * The raw text is split into sentences and each sentence is queried separately.
    * Because of the removed stop words the match position might be up to [[HyperParameters.maxDistanceBetweenNgrams]] bigger.
    * Use the findPlagiarismsExtendedText Method to get those also.
    *
    * @param rawText         the raw, unprocessed text(user input)
    * @param hyperParameters the hyper parameters
    * @return a map of positions in the original text to matches
    **/
  //TODO potential improvement- query database once instead of per sentence
  def findPlagiarisms(rawText: String, hyperParameters: HyperParameters): Map[TextPosition, List[Match]] = {

    //split into sentences
    val inputAsRawSentences = rawText.split("[.,!?]")


    //get the positions
    var positions = inputAsRawSentences.map(x => {
      val start = rawText.indexOf(x)
      //+1 because of the punctuation mark
      new TextPosition(start, start + x.length + 1)
    })
    //remove the last punctuation mark if necessary
    if (!".,!?".contains(rawText.last))
      positions.last.end = positions.last.end - 1

    //split into tokens using the inverse index builder
    val sentencesTokenized = inputAsRawSentences.
      map(sentenceRawText => InverseIndexBuilderImpl.tokenizeAndNormalize(sentenceRawText))

    //merge short sentences with positions
    val mergedSentencesWithPositions = mergeSentences(sentencesTokenized.zip(positions), hyperParameters.minimumSentenceLength)

    val mergedSentences = mergedSentencesWithPositions.map(x => x._1)
    positions = mergedSentencesWithPositions.map(x => x._2).toArray

    //build n-gram hashes
    val ngramsBySentence = mergedSentences.map(entry => entry.filterNot(x => StopWords.stopWords.contains(x)).sliding(n).map(x => InverseIndexHashing.hash(x)).toList)

    //check each sentence for plagiarisms
    val sentenceAndMatches = positions.zip(ngramsBySentence).
      map(entry => (entry._1, findPlagiarismCandidates(entry._2, hyperParameters))).toMap

    sentenceAndMatches
  }

  /**
    * Merges the given sentences so each one has a length of at least minimumSentenceLength.
    *
    * @param sentences             the sentences to merge
    * @param minimumSentenceLength the minimum length of a sentence
    **/
  //TODO last sentence can be shorter than this length right now
  private def mergeSentences(sentences: Array[(List[String], TextPosition)], minimumSentenceLength: Int): List[(List[String], TextPosition)] = {
    var resultStack = new mutable.Stack[(List[String], TextPosition)]
    sentences.foreach(
      (current) => {
        if (resultStack.isEmpty)
        //just add it and continue with the next one
          resultStack.push(current)
        else {
          val previous = resultStack.top
          if (previous._1.size >= minimumSentenceLength) {
            //the previous one is long enough, just add the next
            resultStack.push(current)
          } else {
            //the previous one is shorter, combine it with the current one
            val combinedWords = previous._1 ::: current._1
            val textPosition = previous._2 + current._2
            resultStack.pop()
            resultStack.push((combinedWords, textPosition))
          }
        }
      })
    //we have to reverse the stack so that the first sentences are first
    resultStack.reverse.toList
  }

  /**
    * Finds potential matches of the given sentence split in n-grams-hashes.
    * It's not necessary to be a sentence, whatever list of n-gram-hashes will be queried against the database and
    * evaluated according to the [[HyperParameters]] but semantically it makes sense.
    *
    * @param ngram_hashes    a sentence as a list of n-gram-hashes
    * @param hyperParameters the hyper parameters. For more info about how they are used see [[HyperParameters]]
    * @return A List of [[Match]]. If there were no matches the list will be empty and the sentence is not a plagiarism.
    *
    **/
  private def findPlagiarismCandidates(ngram_hashes: List[Long], hyperParameters: HyperParameters): List[Match] = {
    if (ngram_hashes == null || ngram_hashes.isEmpty)
      return List.empty[Match]

    //query the database to get the n-gram hashes
    val result = cassandraClient.queryNGramHashesAsArray(ngram_hashes)
    val ngramsSetSize = ngram_hashes.toSet.size

    //remove all below the first threshold
    val candidates = getCandidatesBasedOnThreshold(result, ngramsSetSize, hyperParameters.threshold)

    if (candidates.isEmpty)
    // no candidates, sentence is not a plagiarism, return an empty match
      return List.empty[Match]

    //filter all non candidates
    val filtered = result.filter(row => candidates.contains(row.getInt(InverseIndexTable.DocId)))

    //convert cassandra row to scala types, group by docIds and flatten occurrences
    val ngramsByDocId = getNgramsByDocId(filtered)

    //apply remaining hyperparameter constraints and split into matches
    val matches = getMatches(ngramsByDocId, ngramsSetSize, hyperParameters)
    matches
  }

  /**
    * @return documents for which set(ngrams).size/ngramSize is above the threshold.
    **/
  private def getCandidatesBasedOnThreshold(databaseResult: Array[CassandraRow], ngramsSize: Int, threshold: Double): Map[Int, Int] = {
    //get the number of matches for each document
    val ngramSetSizesByDocIds = databaseResult.groupBy(row => row.getInt(InverseIndexTable.DocId))
      .map(entry => (entry._1, entry._2.map(row => row.getLong(InverseIndexTable.NGram)).toSet.size))

    //remove all below the threshold
    val candidates = ngramSetSizesByDocIds.filter(entry => (entry._2 / ngramsSize.toDouble) > threshold)
    candidates
  }

  /**
    * Gets the scala values from the CassandraRow, expands the occurrences,
    * and groups the n-gram-hashes by docIds.
    *
    * Example:
    * IN: CassandraRow{ngram_hash:[1234567] docId:1,occurrences:[12,266]}
    * OUT: 1: ngram[1234567], occurrence: 12
    * 1: ngram[1234567], occurrence: 266
    *
    * @param candidates rows of candidate plagiarism documents
    * @return the n-gram-hashes by docId
    **/
  private def getNgramsByDocId(candidates: Array[CassandraRow]): Map[Int, mutable.MutableList[(Long, Int)]] = {

    candidates.foldLeft(Map.empty[Int, mutable.MutableList[(Long, Int)]]) {
      (docIdsToHashesAndOccurencesTuplesMap, row) => {
        val docId = row.getInt(InverseIndexTable.DocId)
        val ngram_hash = row.getLong(InverseIndexTable.NGram)
        val occurrences = row.get[List[Int]](InverseIndexTable.Occurrences)

        var ngramsList = docIdsToHashesAndOccurencesTuplesMap.getOrElse(docId, new mutable.MutableList[(Long, Int)])
        occurrences.foreach(occurence => ngramsList.+=((ngram_hash, occurence)))

        docIdsToHashesAndOccurencesTuplesMap.updated(docId, ngramsList)
      }
    }
  }

  /**
    * Returns the potential plagiarism matches from it's n-gram-hashes to docIds result from the database
    * according to the [[HyperParameters]].
    *
    * @return A List of [[Match]]. If there were no matches the list will be empty and the sentence is not a plagiarism.
    *         The [[Match.positon.start]]is the start position in the wikipedia article with docId [[Match.docId]]
    *         and the [[Match.positon.end]] is the end(excluded)
    */
  private def getMatches(ngramsByDocId: Map[Int, mutable.MutableList[(Long, Int)]],
                         ngramsSize: Int, hyperParameters: HyperParameters): List[Match] = {
    ngramsByDocId.foldLeft(List.empty[Match]) {
      (accumulator, x) => {
        val candidatesStack = buildCandidatesStack(x, hyperParameters)
        val matches = filterAndBuildMatches(candidatesStack, ngramsSize, x._1, hyperParameters)
        accumulator ::: matches
      }
    }
  }

  /** Builds candidate plagiarism segments(lists of n-grams ) split by max distance */
  private def buildCandidatesStack(x: (Int, mutable.MutableList[(Long, Int)]), hyperParameters: HyperParameters):
  mutable.Stack[mutable.MutableList[(Long, Int)]] = {
    val candidatesStack = new mutable.Stack[mutable.MutableList[(Long, Int)]]
    val sortedNgrams = x._2.sortBy(x => x._2)
    sortedNgrams.foreach(
      (current) => {
        if (candidatesStack.isEmpty)
          candidatesStack.push(mutable.MutableList(current))
        else {
          val previousList = candidatesStack.top
          val previous = previousList.last

          if (Math.abs(previous._2 - current._2) <= hyperParameters.maxDistanceBetweenNgrams) {
            previousList += current
          } else {
            candidatesStack.push(mutable.MutableList(current))
          }
        }
      })
    candidatesStack
  }

  /** applies the secondaryThreshold and maxAverageDistance hyperparameter constraints */
  private def filterAndBuildMatches(candidatesStack: mutable.Stack[mutable.MutableList[(Long, Int)]],
                                    ngramsSize: Int, docId: Int, hyperParameters: HyperParameters): List[Match] = {
    candidatesStack.filter(entry => {
      val isAboveThreshold = (entry.size / ngramsSize.toDouble) > hyperParameters.secondaryThreshold

      //accumulator_.1- the total distance, ._2 the previous position
      val totalDistance = entry.foldLeft((0, entry.head._2)) {
        (accumulator, current) =>
          val distance = current._2 - accumulator._2
          (accumulator._1 + distance, current._2)

      }._1
      val avgDistance = totalDistance / entry.size
      val isBelowMaxAverageDistance = avgDistance <= hyperParameters.maxAverageDistance
      isAboveThreshold && isBelowMaxAverageDistance
    }).map(candidate => new Match(new TextPosition(candidate.head._2, candidate.last._2 + 1), docId)).toList
  }

  //positions local to tokenized text
  /*get stop words before and after and add them to the result*/
  private def expandToIncludeStopWordsAsText(m: Match, matchWikiText: Vector[String], maxDistanceBetweenNgrams: Int): String = {
    var matchTokens = matchWikiText.slice(m.positon.start, m.positon.end)

    //clip it so we don't have negative positions
    var stopWordsBeforeStartPosition = Math.max(0, m.positon.start - maxDistanceBetweenNgrams)

    //stop when a non stop word is reached starting from the end. Reverse again after to have correct ordering
    var stopWordsBefore = matchWikiText.slice(stopWordsBeforeStartPosition, m.positon.start).reverse.takeWhile(StopWords.stopWords.contains).reverse

    //clip it so we don't have positions more than elements
    var StopWordsAfterEndPosition = Math.min(matchWikiText.length, m.positon.end + maxDistanceBetweenNgrams)
    var stopWordsAfter = matchWikiText.slice(m.positon.end, StopWordsAfterEndPosition) takeWhile StopWords.stopWords.contains

    //concatenate them
    var resultMatch = stopWordsBefore ++ matchTokens ++ stopWordsAfter

    resultMatch.mkString(" ")
  }

}