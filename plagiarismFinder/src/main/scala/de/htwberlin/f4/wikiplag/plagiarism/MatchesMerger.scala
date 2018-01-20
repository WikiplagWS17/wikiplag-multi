package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.{Match, TextPosition}

import scala.collection.mutable

/** Merges text position which are directly after each other. handles their matches aswell.
  */
object MatchesMerger {
  def mergeMatches(mathes: Map[TextPosition, List[Match]]): Map[TextPosition, List[Match]] = {
    if (mathes.isEmpty)
      return mathes

    //sorted in ascending order by text position
    mathes.toSeq.sortBy(x => x._1.start).foldLeft(new mutable.Stack[(TextPosition, List[Match])]) {
      (accumulator, current) => {
        if (accumulator.isEmpty)
          accumulator.push(current)
        else {
          var topPosition = accumulator.top._1
          var currentPosition = current._1
          //if directly following each other merge
          if (topPosition.end + 1 == currentPosition.start) {
            var previousMatches = accumulator.top._2
            accumulator.pop

            var currentMatches = current._2
            accumulator.push((new TextPosition(topPosition.start, currentPosition.end), mergeMatchesIfNecessary(previousMatches ::: currentMatches)))
          } else {
            accumulator.push(current)
          }
        }
      }
    }.toMap
  }

  def mergeMatchesIfNecessary(matches: List[Match]): List[Match] = {
    matches.groupBy(x => x.docId).
      flatMap(m => mergePositionsNextToEachOther(m._2.map(x => x.positon)).map(position => new Match(position, m._1)))
      .toList
  }

  def mergePositionsNextToEachOther(list: List[TextPosition]): List[TextPosition] = {
    list.sortBy(x => x.start).foldLeft(new mutable.Stack[(TextPosition)]) {
      (accumulator, current) => {
        if (accumulator.isEmpty)
          accumulator.push(current)
        else {
          var top = accumulator.top
          //if close next to each other merge
          if (Math.abs(top.end + 1 - current.start) < 10) {
            accumulator.pop
            accumulator.push(new TextPosition(top.start, current.end))
          } else {
            accumulator.push(current)
          }
        }
      }
    }.toList
  }
}