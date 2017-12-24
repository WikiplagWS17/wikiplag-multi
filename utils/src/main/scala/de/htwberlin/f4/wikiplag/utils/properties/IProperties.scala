package de.htwberlin.f4.wikiplag.utils.properties

/**
  * Contains various meta information.
  */
trait IProperties {

  /**
    * Returns true if the Spark App has been launched from an IDE.
    */
  def runningFromIDE(): Boolean
}
