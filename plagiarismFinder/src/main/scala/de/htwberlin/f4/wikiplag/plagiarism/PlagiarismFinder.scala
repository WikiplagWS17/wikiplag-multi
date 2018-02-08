package de.htwberlin.f4.wikiplag.plagiarism

import com.datastax.spark.connector.CassandraRow
import de.htwberlin.f4.wikiplag.plagiarism.models.{HyperParameters, Match, TextPosition}
import de.htwberlin.f4.wikiplag.plagiarism.services.PlagiarismMatchesMergeService
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable
import de.htwberlin.f4.wikiplag.utils.inverseindex.{InverseIndexBuilderImpl, InverseIndexHashing, StopWords}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Carries out a plagiarism analysis to determining if a given text has parts which are potential plagiarisms from wikipedia.
  *
  * @author
  * Anton K.
  * @constructor Creates a new instance.
  *
  * @param cassandraClient the cassandra client
  * @param n               the n used to build the n-grams
  *
  */
class PlagiarismFinder(val cassandraClient: CassandraClient, n: Int = 4) {

  /** Returns potential plagiarism matches for the given raw text input.
    *
    * @param rawText         the raw, unprocessed text(user input)
    * @param hyperParameters the hyper parameters
    *
    * @return a map from text positions in the raw text to a list of plagiarism text from wikipedia, docId tuples
    */
  def findPlagiarisms(rawText: String, hyperParameters: HyperParameters): Map[TextPosition, List[(Vector[String], Int)]] = {
    val potentialPlagiarimsAsDatabasePositions = findPlagiarismsDatabasePositions(rawText, hyperParameters)

    if (potentialPlagiarimsAsDatabasePositions.isEmpty)
      return Map.empty[TextPosition, List[(Vector[String], Int)]]

    val docIds = potentialPlagiarimsAsDatabasePositions.flatMap(x => x._2).map(x => x.docId).toList
    val docIdsToDocsMap = cassandraClient.queryDocIdsTokensAsMap(docIds)

    val result = potentialPlagiarimsAsDatabasePositions.map(textPlagiarismPair => textPlagiarismPair._1 ->
      textPlagiarismPair._2.map(m => {
        (getTextFromMatchPosition(m.positon, docIdsToDocsMap(m.docId), n),
          m.docId)
      }))

    result
  }

  /**
    * Returns potential plagiarism matches for the given raw text input.
    * The raw text is split into sentences and each sentence is queried separately.
    * The result matches return the positions of the n-grams from the database which aren't exactly the matches - they
    * include only the first word of the n-gram. The end of the real matching text position is the position + n-1 non stop words after it.
    *
    * @param rawText         the raw, unprocessed text(user input)
    * @param hyperParameters the hyper parameters
    *
    * @return a map of positions in the rawText to matches
    **/
  private def findPlagiarismsDatabasePositions(rawText: String, hyperParameters: HyperParameters): Map[TextPosition, List[Match]] = {

    //split into sentences
    val inputAsRawSentences = rawText.split("[.,!?]")

    //get the positions
    var positions = inputAsRawSentences.foldLeft(ListBuffer.empty[TextPosition], 0) {
      (accumulator, sentence) => {
        val previousEnd = accumulator._2
        val listBuffer = accumulator._1
        val start = rawText.indexOf(sentence, previousEnd)
        val end = start + sentence.length
        listBuffer.append(new TextPosition(start, end))
        (listBuffer, end)
      }
    }._1.toArray

    //split into tokens using the inverse index builder
    val sentencesTokenized = inputAsRawSentences.
      map(sentenceRawText => InverseIndexBuilderImpl.tokenizeAndNormalize(sentenceRawText))

    //merge short sentences with positions
    val mergedSentencesWithPositions = mergeSentences(sentencesTokenized.zip(positions), hyperParameters.minimumSentenceLength)

    val mergedSentences = mergedSentencesWithPositions.map(x => x._1)
    positions = mergedSentencesWithPositions.map(x => x._2).toArray

    //build n-gram hashes
    //the filter is required because the last element of sliding sliding may be of size <n
    val ngramsBySentence = mergedSentences.map(entry => entry.filterNot(x => StopWords.stopWords.contains(x)).sliding(n).filter(_.size == n).map(x => InverseIndexHashing.hash(x)).toList)

    //while debugging uncomment this to see the values as n-grams isntead of n-gram hashes
    //val tmp = mergedSentences.map(entry => entry.filterNot(x => StopWords.stopWords.contains(x)).sliding(n).filter(_.size==n).map(x => x).toList)

    //get all hashes from the database in a single query
    val nGramHashesFromDatabase = cassandraClient.queryNGramHashesAsArray(ngramsBySentence.flatten)

    //check each sentence for plagiarisms
    val sentenceAndMatches = positions.zip(ngramsBySentence).
      map(entry => (entry._1, findPlagiarismCandidates(entry._2, hyperParameters, nGramHashesFromDatabase))).toMap

    //filter out empty matches
    val filtered = sentenceAndMatches.filter(x => x._2.nonEmpty)

    //merge adjacent matches
    PlagiarismMatchesMergeService.mergeMatches(filtered)
  }

  /**
    * Merges the given sentences so each one has a length of at least minimumSentenceLength.
    *
    * @param sentences             the sentences to merge
    * @param minimumSentenceLength the minimum length of a sentence
    **/
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
            val textPosition = new TextPosition(previous._2.start, current._2.end)
            resultStack.pop()
            resultStack.push((combinedWords, textPosition))
          }
        }
      })

    //ensure the last one also fulfils the condition
    if (resultStack.size >= 2)
      if (resultStack.top._1.size < minimumSentenceLength) {
        val lastAdded = resultStack.pop()
        val beforeLastAdded = resultStack.pop()
        val combinedWords = beforeLastAdded._1 ::: lastAdded._1
        val textPosition = new TextPosition(beforeLastAdded._2.start, lastAdded._2.end)
        resultStack.push((combinedWords, textPosition))
      }

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
    *
    * @return A List of [[Match]]. If there were no matches the list will be empty and the sentence is not a plagiarism.
    *
    **/
  private def findPlagiarismCandidates(ngram_hashes: List[Long], hyperParameters: HyperParameters, nGramHashesFromDatabase: Array[CassandraRow]): List[Match] = {
    if (ngram_hashes == null || ngram_hashes.isEmpty)
      return List.empty[Match]

    //fetch only the relevant ones to the current sentence
    var hashesSet = ngram_hashes.toSet
    val result = nGramHashesFromDatabase.filter(x => hashesSet contains x.getLong(InverseIndexTable.NGram))
    //cassandraClient.queryNGramHashesAsArray(ngram_hashes)

    val ngramsSetSize = ngram_hashes.toSet.size

    //remove all below the first threshold
    val candidates = getCandidatesBasedOnThreshold(result, ngramsSetSize, hyperParameters.HashesInDocumentThreshold)

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
    */
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
    *
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

  /** applies the secondaryThreshold and maxAverageDistance hyper parameter constraints */
  private def filterAndBuildMatches(candidatesStack: mutable.Stack[mutable.MutableList[(Long, Int)]],
                                    ngramsSize: Int, docId: Int, hyperParameters: HyperParameters): List[Match] = {
    candidatesStack.filter(entry => {
      val isAboveThreshold = (entry.size / ngramsSize.toDouble) > hyperParameters.HashesInSentenceThreshold

      //accumulator_.1- the total distance, ._2 the previous position
      val totalDistance = entry.foldLeft((0, entry.head._2)) {
        (accumulator, current) =>
          val distance = current._2 - accumulator._2
          (accumulator._1 + distance, current._2)

      }._1
      val avgDistance = totalDistance / entry.size
      val isBelowMaxAverageDistance = avgDistance <= hyperParameters.maxAverageDistance
      isAboveThreshold && isBelowMaxAverageDistance
    }).map(candidate => new Match(new TextPosition(candidate.head._2, candidate.last._2), docId)).toList
  }

  /** Gets the wikipedia text for the given text position from the wikipedia article text. In order to do this
    * the n-1 non-stop words after the end position must also be included.
    *
    * @param position      the position in the wikipedia article
    * @param matchWikiText the tokenized wikipedia text.
    * @param n             the n used fo the n-grams
    */
  private def getTextFromMatchPosition(position: TextPosition, matchWikiText: Vector[String], n: Int): Vector[String] = {

    //all words in the text after the match
    val afterMatch = matchWikiText.drop(position.end + 1)

    //takes words while n-1 non stop words have been taken
    var nonStopWordsCounter = n - 1
    val endTokens = afterMatch.takeWhile(x => {
      var shouldReturn = nonStopWordsCounter != 0
      if (!StopWords.stopWords.contains(x))
        nonStopWordsCounter -= 1
      shouldReturn
    })
    //including start, excluding end
    val matchFromPostions = matchWikiText.slice(position.start, position.end + 1)
    //concatenate them
    matchFromPostions ++ endTokens
  }
}