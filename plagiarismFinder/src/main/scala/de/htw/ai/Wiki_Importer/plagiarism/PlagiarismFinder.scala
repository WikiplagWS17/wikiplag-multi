package de.htw.ai.Wiki_Importer.plagiarism

import com.datastax.spark.connector.CassandraRow
import de.htw.ai.Wiki_Importer.utils.InverseIndexBuilderImpl
import de.htw.ai.Wiki_Importer.utils.database.{CassandraClient, InverseIndexTable}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Represents one potential plagiarism
  *
  * @param start the start of the plagiarism
  * @param end   the end of the plagiarism
  */
class Match(val start: Long, val end: Long, val docId: Long) {
  override def toString: String = s"[Start:\t $start \n End:\t $end \n"
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
  * 09.12.2017
  *
  * A class for finding determining if a given text is a wikipedia plagiarism.
  * @constructor Creates a new instance.
  * @param cassandraHost the ip-address of the cassandra database host
  * @param cassandraPort the port on which the database can be reached
  * @param cassandraUser the username for the cassandra database
  * @param cassandraPW   the password for the user
  **/
//TODO cassandra keyspace, tables etc as a objects similar to InverseIndexTable but with fields for tableName and Keyspace
class PlagiarismFinder(cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, keyspace: String, articlesTable: String, invIndexTable: String, n: Int = 4) {
  val sparkConf = new SparkConf(true).setAppName("Database_Benchmark_Cassandra")
    .set("spark.cassandra.connection.host", cassandraHost)
    .set("spark.cassandra.connection.port", cassandraPort.toString)
    .set("spark.cassandra.auth.username", cassandraUser)
    .set("spark.cassandra.auth.password", cassandraPW).setMaster("local")
  val sc = new SparkContext(sparkConf)
  val cassandraClient = new CassandraClient(sc, keyspace, invIndexTable, articlesTable)

  /**
    * Returns potential plagiarism matches to the given raw text input.
    * The raw text is split into sentences and each sentence is queried separately.
    *
    * @param rawText         the raw, unprocessed text(user input)
    * @param hyperParameters the hyper parameters
    * @return a map of word tokens to matches
    **/
  //TODO potential improvement- query database once instead of per sentence
  //TODO return sentence position or sentence to matches
  def findPlagiarisms(rawText: String, hyperParameters: HyperParameters): Map[List[String], List[Match]] = {

    //split into sentences
    val inputAsRawSentences = rawText.split("[.,!?]")

    //split into tokens using the inverse index builder
    val sentencesTokenized = inputAsRawSentences.
      map(sentenceRawText => InverseIndexBuilderImpl.tokenizeAndNormalize(sentenceRawText))

    //merge short sentences
    val mergedSentences = mergeSentences(sentencesTokenized, hyperParameters.minimumSentenceLength)

    //build n-grams
    val ngramsBySentence = mergedSentences.map(entry => entry.sliding(n).toList)

    //check each sentence for plagiarisms
    val sentenceAndMatches = mergedSentences.zip(ngramsBySentence).
      map(entry => (entry._1, findPlagiarismCandidates(entry._2, hyperParameters))).toMap

    sc.stop()
    sentenceAndMatches
  }

  /**
    * Merges the given sentences so each one has a length of at least minimumSentenceLength.
    *
    * @param sentences             the sentences to merge
    * @param minimumSentenceLength the minimum length of a sentence
    **/
  //TODO last sentence can be shorter than this length right now
  private def mergeSentences(sentences: Array[List[String]], minimumSentenceLength: Int): List[List[String]] = {
    var resultStack = new mutable.Stack[List[String]]
    sentences.foreach(
      (current) => {
        if (resultStack.isEmpty)
        //just add it and continue with the next one
          resultStack.push(current)
        else {
          val previous = resultStack.top
          if (previous.size >= minimumSentenceLength) {
            //the previous one is long enough, just add the next
            resultStack.push(current)
          } else {
            //the previous one is shorter, combine it with the current one
            val combined = previous ::: current
            resultStack.pop()
            resultStack.push(combined)
          }
        }
      })
    //we have to reverse the stack so that the first sentences are first
    resultStack.reverse.toList
  }

  /**
    * Finds potential matches of the given sentence split in n-grams.
    * It's not necessary to be a sentence, whatever list of n-grams will be queried against the database and
    * evaluatied accordng to the [[HyperParameters]] but semantically it makes sense.
    *
    * @param ngrams          a sentence as a list of in n-grams
    * @param hyperParameters the hyper parameters. For more info about how they are used see [[HyperParameters]]
    * @return A List of [[Match]]. If there were no matches the list will be empty and the sentence is not a plagiarism.
    *
    **/
  private def findPlagiarismCandidates(ngrams: List[List[String]], hyperParameters: HyperParameters): List[Match] = {
    if (ngrams == null || ngrams.isEmpty)
      return List.empty[Match]

    //query the database to get the n-grams.
    val result = cassandraClient.query4GramsAsArray(ngrams)
    val ngramsSetSize = ngrams.toSet.size

    //remove all below the first threshold
    val candidates = getCandidatesBasedOnThreshold(result, ngramsSetSize, hyperParameters.threshold)

    if (candidates.isEmpty)
    // no candidates, sentence is not a plagiarism, return an empty match
      List.empty[Match]

    //filter all non candidates
    val filtered = result.filter(row => candidates.contains(row.getLong(InverseIndexTable.DocId)))

    //convert cassandra row to scala types, group by docIds and flatten occurrences
    val ngramsByDocId = getNgramsByDocId(filtered)

    //apply remaining hyperparameter constraints and split into matches
    val matches = getMatches(ngramsByDocId, ngramsSetSize, hyperParameters)
    matches
  }

  /**
    * @return documents for which set(ngrams).size/ngramSize is above the threshold.
    **/
  private def getCandidatesBasedOnThreshold(databaseResult: Array[CassandraRow], ngramsSize: Int, threshold: Double): Map[Long, Int] = {
    //get the number of matches for each document
    val ngramSetSizesByDocIds = databaseResult.groupBy(row => row.getLong(InverseIndexTable.DocId))
      .map(entry => (entry._1, entry._2.map(row => row.getTupleValue(InverseIndexTable.NGram)).toSet.size))

    //remove all below the threshold
    val candidates = ngramSetSizesByDocIds.filter(entry => (entry._2 / ngramsSize.toDouble) > threshold)
    candidates
  }

  /**
    * Gets the scala values from the CassandraRow, expands the occurrences,
    * and groups the n-grams by docId.
    *
    * Example:
    * IN: CassandraRow{ngram:["the","bridge","of","death"] docId:1,occurrences:[12,266]}
    * OUT: 1: ngram["the","bridge","of","death"], occurrence: 12
    * 1: ngram["the","bridge","of","death"], occurrence: 266
    *
    * @param candidates n-gram rows of candidate plagiarism documents
    * @return the n-grams by docId
    **/
  private def getNgramsByDocId(candidates: Array[CassandraRow]): Map[Long, mutable.MutableList[(List[String], Int)]] = {
    //the docid
    candidates.foldLeft(Map.empty[Long, mutable.MutableList[(List[String], Int)]]) {
      (accumulator, row) => {

        val docId = row.getLong(InverseIndexTable.DocId)
        val ngram = row.getTupleValue(InverseIndexTable.NGram).values.map(value => value.asInstanceOf[String]).toList
        val occurences = row.get[List[Int]](InverseIndexTable.Occurences)

        var ngramsList = accumulator.getOrElse(docId, new mutable.MutableList[(List[String], Int)])
        occurences.foreach(occurence => ngramsList.+=((ngram, occurence)))

        accumulator.updated(docId, ngramsList)
      }
    }
  }

  /**
    * Returns the potential plagiarism matches from it's n-grams to docIds result from the database
    * according to the [[HyperParameters]].
    *
    * @return A List of [[Match]]. If there were no matches the list will be empty and the sentence is not a plagiarism.
    *         The [[Match.start]]is the start position in the wikipedia article with docId [[Match.docId]
    *         and the [[Match.end]] is the end
    */
  private def getMatches(ngramsByDocId: Map[Long, mutable.MutableList[(List[String], Int)]],
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
  private def buildCandidatesStack(x: (Long, mutable.MutableList[(List[String], Int)]), hyperParameters: HyperParameters):
  mutable.Stack[mutable.MutableList[(List[String], Int)]] = {
    val candidatesStack = new mutable.Stack[mutable.MutableList[(List[String], Int)]]
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
  private def filterAndBuildMatches(candidatesStack: mutable.Stack[mutable.MutableList[(List[String], Int)]],
                                    ngramsSize: Int, docId: Long, hyperParameters: HyperParameters): List[Match] = {
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
    }).map(candidate => new Match(candidate.head._2, candidate.last._2, docId)).toList
  }
}