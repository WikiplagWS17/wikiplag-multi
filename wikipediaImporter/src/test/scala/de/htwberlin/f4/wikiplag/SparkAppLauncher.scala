package de.htwberlin.f4.wikiplag

/** a neat trick to be able to launch the SparkApp which has % provided dependencies from idea.
  *
  * Since some dependencies (spark, spark-sql) are marked as provided they are only available during compile and test,
  * but not during runtime. The reason they are marked as provided is because the production environment already has them.
  * But each time the App is started from the IDE a runtime exception is thrown - Class not found, since the dependencies
  * aren't part of the development machine.
  * To bypass this we just start the App from Test and this way the provided dependencies are also included at runtime.
  */
object SparkAppLauncher {
  def main(args: Array[String]) {
    SparkApp.main(args)
  }
}
