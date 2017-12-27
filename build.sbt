/** SBT multi-project config
  *
  * For more info on sbt multi-projects see:
  * http://www.scala-sbt.org/1.0/docs/Multi-Project.html
  * and https://github.com/sbt/sbt-assembly multi project section
  *
  * The sbt assembly plugin is used to deploy jars with dependencies (fat jars).
  * For more info on the assembly plugin see:
  * https://github.com/sbt/sbt-assembly
  *
  */

/* ************************************************************************* *\
|                      Shared Dependencies and Settings                       |
\* ************************************************************************* */

//common settings such as name, version, scala version etc.
lazy val commonSettings = Seq(
  organization := "de.htwberlin.f4.wikiplag",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  parallelExecution in test := false
)

//testing dependencies
lazy val testDependencies = Seq(
  //not sure if this is required
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

//spark context, spark config ...
lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
)

//dataframe ...
lazy val sparkSQLDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"
)

//allows communication with a cassandra database from spark
//https://github.com/datastax/spark-cassandra-connector
lazy val cassandraDependencies = Seq(
  "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11"
)

/* ************************************************************************* *\
|                             UTILS Project                                   |
\* ************************************************************************* */
lazy val utils = (project in file("utils"))
  .settings(
    commonSettings,
    name := "WikiPlagUtils",
    // other settings
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= sparkDependencies,
    libraryDependencies ++= sparkSQLDependencies,
    libraryDependencies ++= cassandraDependencies,
    libraryDependencies ++= Seq(
      //used for parsing xml by the WikidumpParser
      "org.unbescape" % "unbescape" % "1.1.4.RELEASE",
      "com.databricks" % "spark-xml_2.11" % "0.4.1",
      //read config files easily
      "com.typesafe" % "config" % "1.3.1"
    )
  )

/* ************************************************************************* *\
|                             ANALYSER Project                                |
\* ************************************************************************* */
lazy val plagiarismFinder = (project in file("plagiarismFinder"))
  .settings(
    commonSettings,
    name := "WikiPlagAnalyser",
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= sparkDependencies,
    libraryDependencies++=sparkSQLDependencies,
    libraryDependencies ++= Seq(
      //add other dependencies for the analyser project here
    )
  ).dependsOn(utils)


/* ************************************************************************* *\
|                        WIKIPEDIA-IMPORTER Project                           |
\* ************************************************************************* */
lazy val wikipediaImporter = (project in file("wikipediaImporter"))
  .settings(
    commonSettings,
    name := "WikiPlagImporter",
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= sparkDependencies,
    libraryDependencies ++= sparkSQLDependencies,
    libraryDependencies ++= cassandraDependencies,
    libraryDependencies ++= Seq(
      //command line arguments parsing helper
      "commons-cli" % "commons-cli" % "1.2"
      //add other dependencies for the importer project here
    ),
    //configuration for the sbt-assembly plugin
    assemblyJarName in assembly := "wiki_importer.jar",
    mainClass in assembly := Some("de.htwberlin.f4.wikiplag.SparkApp")
  ).dependsOn(utils)


/* ************************************************************************* *\
|                                REST-API Project                             |
\* ************************************************************************* */
//TODO not yet added to multi-project
lazy val restApi = (project in file("restApi"))
  .settings(
    commonSettings,
    name := "WikiPlagRestAPI",
    libraryDependencies ++= testDependencies
    // other settings
  ).dependsOn(utils, plagiarismFinder)