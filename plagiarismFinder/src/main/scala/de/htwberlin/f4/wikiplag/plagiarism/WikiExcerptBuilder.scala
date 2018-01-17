package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.{TextPosition, WikiExcerpt, WikiPlagiarism}
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient

import scala.collection.immutable.ListMap

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
      val excerts=x._1._2.map(x => {
        val document = documentsMap(x._2)
        val excerptTuple = findExactWikipediaExcerpt(x._1, document.text, n)
        val excerpt = buildDisplayExcrept(excerptTuple._1, excerptTuple._2, excerptTuple._3)
        new WikiExcerpt(document.title,document.docId, startInInputText, endInInputText, excerpt)
      })

      new WikiPlagiarism(x._2,excerts)
    }).toList
  }


  private def buildDisplayExcrept(before: String, plagiarism: String, after: String): String = {
    //specifying the span class in the rest api is absolutely disgusting..
    var span=""" <span class="wiki_plag">""""
    s"[...] $before $span$plagiarism $after</span> [...]"
  }

  /** Finds the exact excerpt for the tokenized match from the given wikipedia article.
    *
    * @param tokenizedMatch the tokenzied match
    * @param wikiText       the wikipedia article from which the tokenized match originated
    * @param n              number of words before and after the plagiarism text
    * @return a tuple of (n-words before, plagiarism, n-words after)
    * */
  //TODO. n can be the just whitespaced tokens before and after or even just characters
  private def findExactWikipediaExcerpt(tokenizedMatch: Vector[String], wikiText: String, n: Int): Tuple3[String, String, String] = {
    ("", tokenizedMatch.mkString(" "), "")
  }
}