package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.{TextPosition, WikiExcerpt}
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient

/** Builds wikipedia excerpts from the result of the plagiarism finder so they can be served by the rest api. */
class WikiExcerptBuilder(cassandraClient: CassandraClient) {

  /** Builds wikipedia excerpts from the given plagiarism candidates.
    *
    * @param plagiarismCandidates the result of the [[PlagiarismFinder.findPlagiarisms()]] Method
    * @param n                    number of words to include before and after plagiarism
    * */
  def buildWikiExcerpts(plagiarismCandidates: Map[TextPosition, List[(Vector[String], Int)]], n: Int): Vector[WikiExcerpt] = {

    var docIds = plagiarismCandidates.values.flatten.map(_._2)
    var documentsMap = cassandraClient.queryArticlesAsMap(docIds)

    plagiarismCandidates.flatMap(x => {
      val startInInputText = x._1.start
      val endInInputText = x._1.end

      x._2.map(x => {
        val document = documentsMap(x._2)
        val excerptTuple = findExactWikipediaExcerpt(x._1, document.text, n)
        val excerpt = buildDisplayExcrept(excerptTuple._1, excerptTuple._2, excerptTuple._3)
        new WikiExcerpt(document.title, startInInputText, endInInputText, excerpt)
      })
    }).toVector
  }

  private def buildDisplayExcrept(before: String, plagiarism: String, after: String): String = {
    //specifying the span class in the rest api is absolutely disgusting..
    s"[...] $before <span class=\"wiki_plag\">$plagiarism</span> $after [...]"
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
