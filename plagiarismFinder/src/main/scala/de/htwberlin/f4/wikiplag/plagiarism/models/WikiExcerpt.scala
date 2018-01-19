package de.htwberlin.f4.wikiplag.plagiarism.models

/**
  * Represents a potential plagiarism wikipedia excerpt.
  *
  * @param title   the title from wikipedia
  * @param start   the starting position in the input text (inclusive)
  * @param end     the end position in the input text (exclusive)
  * @param excerpt the wikipedia excerpt
  *
  */
@SerialVersionUID(1)
class WikiExcerpt(val title: String,val id:Int, val start: Int, val end: Int, val excerpt: String) extends Serializable {

  override def toString = s"WikiExcerpt(title=$title, start=$start, end=$end, excerpt=$excerpt,id=$id)"
}
