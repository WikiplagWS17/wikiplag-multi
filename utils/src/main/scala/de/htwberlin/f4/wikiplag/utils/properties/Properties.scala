package de.htwberlin.f4.wikiplag.utils.properties

object Properties extends IProperties {

  override def runningFromIDE(): Boolean = {

    val classPath = System.getProperty("java.class.path")
    //if the idea_rt.jar is present in the class path the spark app has been started within idea
    val runningfromIDE = classPath.contains("idea_rt.jar")
    runningfromIDE
  }
}
