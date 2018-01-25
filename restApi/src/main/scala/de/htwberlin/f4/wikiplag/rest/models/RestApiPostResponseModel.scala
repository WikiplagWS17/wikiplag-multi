package de.htwberlin.f4.wikiplag.rest.models

import de.htwberlin.f4.wikiplag.plagiarism.models.{WikiExcerpt, WikiPlagiarism}
import de.htwberlin.f4.wikiplag.utils.Functions

/** transform to mock.json format */
@SerialVersionUID(1)
case class RestApiPostResponseModel(var plags: List[WikiPlagiarism],var tagged_input_text:String="") extends Serializable {

  def InitTaggedInputTextFromRawText(rawText:String) {
    if (rawText.isEmpty)
      tagged_input_text = ""

    if (plags.isEmpty)
      tagged_input_text = rawText
    else {

      var startEndPositionsLists = plags.flatMap(x=>x.wiki_excerpts).map(x => (x.start, x.end)).unzip
      //concatenate them
      var plagIndices = (startEndPositionsLists._1 ::: startEndPositionsLists._2).distinct.sorted

      var rawTextSplit = Functions.SplitByMultipleIndices(plagIndices, rawText)

      var span ="""<span id="%d" class="input_plag">%s</span>"""

      tagged_input_text= rawTextSplit.zipWithIndex.map(x => {
        //odd ones are plagiarisms
        if (x._2 % 2 == 0)
          // divide by 2 to get the correct index because we have 2 positions per word
          span.format(x._2 / 2, x._1)
        else
          x._1
      }).mkString("")
    }
  }

  }
