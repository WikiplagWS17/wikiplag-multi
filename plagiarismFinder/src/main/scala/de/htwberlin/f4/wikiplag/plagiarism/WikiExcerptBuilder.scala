package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.{TextPosition, WikiExcerpt, WikiPlagiarism}
import de.htwberlin.f4.wikiplag.utils.Functions
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient

/** Builds wikipedia excerpts from the result of the plagiarism finder so they can be served by the rest api. */
class WikiExcerptBuilder(cassandraClient: CassandraClient) {

  /** Builds wikipedia excerpts from the given plagiarism candidates.
    *
    * @param plagiarismCandidates the result of the [[PlagiarismFinder.findPlagiarisms()]] Method
    * @param n                    number of words to include before and after plagiarism
    * */
  def buildWikiExcerpts(plagiarismCandidates: Map[TextPosition, List[(Vector[String], Int)]], n: Int): List[WikiPlagiarism] = {

    var docIds = plagiarismCandidates.values.flatten.map(_._2)
    var documentsMap = cassandraClient.queryArticlesAsMap(docIds)

    plagiarismCandidates.toSeq.sortWith(_._1.start < _._1.start).zipWithIndex.map(x => {
      val startInInputText = x._1._1.start
      val endInInputText = x._1._1.end
      val excerts = x._1._2.map(x => {
        val document = documentsMap(x._2)
        val excerptTuple = findExactWikipediaExcerpt(x._1, document.text, n)
        val excerpt = buildDisplayExcrept(excerptTuple._1, excerptTuple._2, excerptTuple._3)
        new WikiExcerpt(document.title, document.docId, startInInputText, endInInputText, excerpt)
      })

      new WikiPlagiarism(x._2, excerts)
    }).toList
  }


  private def buildDisplayExcrept(before: String, plagiarism: String, after: String): String = {
    //specifying the span class in the rest api is absolutely disgusting..
    var span ="""<span class="wiki_plag">"""
    s"[...] $before$span$plagiarism</span>$after [...]"
  }

  /** Finds the exact excerpt for the tokenized match from the given wikipedia article.
    *
    * @param tokenizedMatch the tokenzied match
    * @param wikiText       the wikipedia article from which the tokenized match originated
    * @param n              number of characters before and after the plagiarism text
    * @return a tuple of (n-words before, plagiarism, n-words after)
    * */
  def findExactWikipediaExcerpt(tokenizedMatch: Vector[String], wikiText: String, n: Int): Tuple3[String, String, String] = {
    try {
      findExactWikipediaExcerptOrThrow(tokenizedMatch, wikiText, n)
    } catch {
      //if there was an exception write the stack trace and just concatenate the tokens
      case e: Exception =>
        e.printStackTrace()
        ("", tokenizedMatch.mkString(" "), "")
    }
  }

  private def findExactWikipediaExcerptOrThrow(tokenizedMatch: Vector[String], wikiText: String, n: Int): Tuple3[String, String, String] = {
    val wikiTextLower = wikiText.toLowerCase
    //find all positions of each word and remove those with no results
    val nonEmptyOrderedTokensPositions = tokenizedMatch.map(x => Functions.allIndicesOf(wikiTextLower, x)).filter(_.nonEmpty).toList

    //less than two words have been found, throw an exception
    if (nonEmptyOrderedTokensPositions.size < 2)
      throw new NoSuchElementException("Less than 2 tokens were found.")

    val firstWordPositions = nonEmptyOrderedTokensPositions.head
    val otherWordsPositions = nonEmptyOrderedTokensPositions.tail

    //build start,end, count of words founds tuples
    val startEndPositionCountTuples = firstWordPositions.map(startingPosition => {
      val endPositionWithNumberOfWordsFound = otherWordsPositions.foldLeft(startingPosition, 0) {
        (accumulator, current) => {
          val previousPosition = accumulator._2
          val closestPositionToPrevious = current.find(_ > previousPosition)
          //no position exists which are after the previous one,
          // continue with the next query word without modifying the positions
          if (closestPositionToPrevious.isEmpty)
            accumulator
          else
            (closestPositionToPrevious.get, accumulator._2 + 1)
        }
      }
      (startingPosition, endPositionWithNumberOfWordsFound._1, endPositionWithNumberOfWordsFound._2)
    })

    //get the one with the most found words
    val best = startEndPositionCountTuples.maxBy(x => x._3)

    //if it has 0 words throw an exception
    if (best._3 == 0)
      throw new NoSuchElementException("0 Element best match.")

    val plagStartPosition = best._1
    val plagEndPosition = best._2
    //clip to avoid out of bounds exceptions
    val beforeStartPosition = math.max(plagStartPosition - n, 0)
    val afterEndPosition = math.min(plagEndPosition + n, wikiText.length - 1)

    val before = wikiText.slice(beforeStartPosition, plagStartPosition)
    val plag = wikiText.slice(plagStartPosition, plagEndPosition)
    val after = wikiText.slice(plagEndPosition, afterEndPosition)

    (before, plag, after)
  }
}