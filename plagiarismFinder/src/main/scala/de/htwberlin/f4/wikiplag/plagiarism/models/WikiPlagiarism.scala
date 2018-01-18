package de.htwberlin.f4.wikiplag.plagiarism.models

@SerialVersionUID(1)
case class WikiPlagiarism(id:Int,wiki_excerpts:List[WikiExcerpt]) extends Serializable {

}
