package de.htwberlin.f4.wikiplag.utils.models

/**
  * Represents a wikipedia document
  *
  * @param docId the doc ID
  * @param text  the raw wikipedia text
  * @param title the wikipedia title
  */
@SerialVersionUID(1)
class Document(val docId: Int, val title: String, val text: String) extends Serializable {

  override def toString = s"Document(title=$title, text=$text, docId=$docId)"
}
