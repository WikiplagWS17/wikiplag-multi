package de.htwberlin.f4.wikiplag.plagiarism.services

import de.htwberlin.f4.wikiplag.plagiarism.models.{Match, TextPosition}

import scala.collection.mutable

/** Merges candidate plagiarism text position in the input text which are directly following each other
  * and have plagiarisms from one and the same document.
  */
object PlagiarismMatchesMergeService {
  /** Merges candidate plagiarism text position in the input text which are directly following each other
    * and have plagiarisms from one and the same document.
    *
    * @param matches the input text plagiarism position to plagiarism map
    */
  def mergeMatches(matches: Map[TextPosition, List[Match]]): Map[TextPosition, List[Match]] = {
    if (matches.isEmpty)
      return matches

    //sorted in ascending order by text position
    matches.toSeq.sortBy(x => x._1.start).foldLeft(new mutable.Stack[(TextPosition, List[Match])]) {
      (accumulator, current) => {

        if (accumulator.isEmpty)
          accumulator.push(current)
        else {
          val topPosition = accumulator.top._1
          val currentPosition = current._1
          //if directly following each other consider merging
          if (topPosition.end + 1 == currentPosition.start) {
            val previousMatches = accumulator.top._2
            val currentMatches = current._2
            val merged = mergeMatchesIfNecessary(previousMatches ::: currentMatches)
            val mergedAtleastOnce = merged._2
            //if atleast one match pair was merged, merge otherwise just push the current one
            if (mergedAtleastOnce) {
              accumulator.pop
              accumulator.push((new TextPosition(topPosition.start, currentPosition.end), merged._1))
            }
            else
            //no merges in the wikipedia positions, just push
              accumulator.push(current)
          } else
          //not following directly, just push
            accumulator.push(current)
        }
      }
    }.toMap
  }

  private def mergeMatchesIfNecessary(matches: List[Match]): (List[Match], Boolean) = {
    var mergedAtleastOnce = false
    val mergedMatches = matches.groupBy(m => m.docId).flatMap(m => {
      val positions = m._2.map(x => x.positon)
      val mergedPositions = mergePositionsNextToEachOther(positions)
      val anyMerged = mergedPositions._2
      if (anyMerged) {
        mergedAtleastOnce = true
        val docId = m._1
        val resultMatches = mergedPositions._1.map(position => new Match(position, docId))
        resultMatches
      } else {
        m._2
      }
    }).toList
    (mergedMatches, mergedAtleastOnce)
  }

  private def mergePositionsNextToEachOther(list: List[TextPosition]): (List[TextPosition], Boolean) = {
    var mergedAtleastOnce = false
    var merged = list.sortBy(x => x.start).foldLeft(new mutable.Stack[(TextPosition)]) {
      (accumulator, current) => {
        if (accumulator.isEmpty)
          accumulator.push(current)
        else {
          var top = accumulator.top
          //if close next to each other merge
          if (((top.end + 1 - current.start) < 20) && (top.end + 1 - current.start) < 20) {
            accumulator.pop
            mergedAtleastOnce = true
            accumulator.push(new TextPosition(top.start, current.end))
          } else {
            accumulator.push(current)
          }
        }
      }
    }.toList
    (merged, mergedAtleastOnce)
  }
}