package de.htwberlin.f4.wikiplag

import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import org.apache.commons.cli.{GnuParser, OptionBuilder, OptionGroup, Options}

/**
  * Encapsulates the command line options.
  */
object CommandLineOptions {

  //throws a parse exception if something goes wrong
  def parse(args: Array[String]): (CassandraParameters, Commands.Command, String) = {
    val options = createCLiOptions()
    val commandLine = new GnuParser().parse(options, args)
    val host = commandLine.getParsedOptionValue("database-host").asInstanceOf[String]
    val port = commandLine.getParsedOptionValue("database-port").asInstanceOf[Number].intValue.toString
    val username = commandLine.getParsedOptionValue("database-username").asInstanceOf[String]
    val password = commandLine.getParsedOptionValue("database-password").asInstanceOf[String]
    val databaseName = commandLine.getParsedOptionValue("database-name").asInstanceOf[String]
    val articlesTable = commandLine.getParsedOptionValue("articles-table").asInstanceOf[String]
    val tokenizedTable = commandLine.getParsedOptionValue("tokenized-table").asInstanceOf[String]
    val inverseIndexTable = commandLine.getParsedOptionValue("inverse-index-table").asInstanceOf[String]

    val parameters = new CassandraParameters(articlesTable, inverseIndexTable, tokenizedTable, databaseName, username, password, host, port)

    if (commandLine.hasOption("extract-wikitext")) {
      val file = commandLine.getParsedOptionValue("extract-wikitext").asInstanceOf[String]
      return (parameters, Commands.ExtractWikiText, file)
    }
    if (commandLine.hasOption("tokenize-extracted-text")) {
      return (parameters, Commands.TokenizeWikiText, null)
    }
    if (commandLine.hasOption("create-inverse-index")) {
      val n = commandLine.getParsedOptionValue("create-inverse-index").asInstanceOf[Number].intValue.toString
      return (parameters, Commands.BuildInverseIndex, n)
    }
    throw new IllegalArgumentException("Unknown action. Did you add a new action but forgot to add handling for it here?")
  }

  private def createCLiOptions() = {
    val options = new Options()

    /*database access options*/
    OptionBuilder.withLongOpt("database-host")
    OptionBuilder.withDescription("Database host")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("host")
    options.addOption(OptionBuilder.create("dh"))

    OptionBuilder.withLongOpt("database-port")
    OptionBuilder.withDescription("Database port")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("port")
    options.addOption(OptionBuilder.create("dp"))

    OptionBuilder.withLongOpt("database-username")
    OptionBuilder.withDescription("Database username")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("user")
    options.addOption(OptionBuilder.create("du"))

    OptionBuilder.withLongOpt("database-password")
    OptionBuilder.withDescription("Database password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("password")
    options.addOption(OptionBuilder.create("dpw"))

    /*database keyspace and table names options*/
    OptionBuilder.withLongOpt("database-name")
    OptionBuilder.withDescription("Database name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("database")
    options.addOption(OptionBuilder.create("dn"))

    OptionBuilder.withLongOpt("articles-table")
    OptionBuilder.withDescription("Wiki Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablea")
    options.addOption(OptionBuilder.create("at"))

    OptionBuilder.withLongOpt("inverse-index-table")
    OptionBuilder.withDescription("inverse Index Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablei")
    options.addOption(OptionBuilder.create("it"))

    OptionBuilder.withLongOpt("tokenized-table")
    OptionBuilder.withDescription("Tokenized Table name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablet")
    options.addOption(OptionBuilder.create("tt"))

    /* Commands */
    val group = new OptionGroup()
    group.setRequired(true)

    OptionBuilder.withLongOpt("extract-wikitext")
    OptionBuilder.withDescription("Path to the XML-File containing the Wiki-Articles")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("wiki-file")
    group.addOption(OptionBuilder.create("e"))

    OptionBuilder.withLongOpt("tokenize-extracted-text")
    OptionBuilder.withDescription("Tokenize the wiki text")
    OptionBuilder.hasArgs(0)
    group.addOption(OptionBuilder.create("t"))

    OptionBuilder.withLongOpt("create-inverse-index")
    OptionBuilder.withDescription("Create the inverse index table with the given n.")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("n")
    group.addOption(OptionBuilder.create("i"))

    options.addOptionGroup(group)
    options
  }

  object Commands {

    val commands = Seq(TokenizeWikiText, BuildInverseIndex, ExtractWikiText)

    sealed trait Command

    /** Represents the command to extract the text from the wikipedia articles. */
    case object ExtractWikiText extends Command

    /** Represents the command to tokenize the already extracted text. */
    case object TokenizeWikiText extends Command

    /** Represents the command to build the inverse index from the tokenized text. */
    case object BuildInverseIndex extends Command

  }

}