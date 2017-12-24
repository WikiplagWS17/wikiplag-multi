

package de.htwberlin.f4.wikiplag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import de.htwberlin.f4.wikiplag.plagiarism.{HyperParameters, PlagiarismFinder}
import de.htwberlin.f4.wikiplag.utils.{InverseIndexBuilderImpl, SparkConfHelper}
import de.htwberlin.f4.wikiplag.utils.parser.WikiDumpParser
import org.apache.commons.cli._


/*Created by Jörn Sattler for Projekt: "Wikiplag"
 *
 * 25.07.2017
 *
 * Modified by Anton K.
 * 18.12.2017
 *
 *Class which uses the WikidumpParser.scala and the InverseIndexBuilderImp.scala
 * to parse the wikipedia.xml dump and write its content to
 * a MySQLDB, a MongoDB, and a CassandraDB. Also builds an inverse Index
 * structure and writes this structure to the databases as well.
 */
//TODO adjust docs
object SparkApp {

  def main(args: Array[String]) {
    val options = createCLiOptions()
    val n = 4

    try {
      val commandLine = new GnuParser().parse(options, args)
      val dbHost = commandLine.getParsedOptionValue("db_host").asInstanceOf[String]
      val dbPort = commandLine.getParsedOptionValue("db_port").asInstanceOf[Number].intValue()
      val dbUser = commandLine.getParsedOptionValue("db_user").asInstanceOf[String]
      val dbPass = commandLine.getParsedOptionValue("db_password").asInstanceOf[String]
      val dbName = commandLine.getParsedOptionValue("db_name").asInstanceOf[String]
      val dbWTab = commandLine.getParsedOptionValue("db_wtable").asInstanceOf[String]
      val dbITab = commandLine.getParsedOptionValue("db_itable").asInstanceOf[String]
      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("e").asInstanceOf[String]
        extractTextCassandra(file, dbHost, dbPort, dbUser, dbPass, dbName, dbWTab)
      } else if (commandLine.hasOption("i")) {
        createInverseIndexCass(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab, n)
      } else if (commandLine.hasOption("bt")) {

        testPlagiarismFinder(new PlagiarismFinder(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab))
      }
    } catch {
      case e: ParseException =>
        println("Unexpected ParseException: " + e.getMessage)
      case e: Exception =>
        e.printStackTrace()

    }
  }

  private def createCLiOptions() = {
    val options = new Options()

    /* CLi Options*/
    OptionBuilder.withLongOpt("db_host")
    OptionBuilder.withDescription("Database Host")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("host")
    options.addOption(OptionBuilder.create("dh"))

    OptionBuilder.withLongOpt("db_port")
    OptionBuilder.withDescription("Database Port")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("port")
    options.addOption(OptionBuilder.create("dp"))

    OptionBuilder.withLongOpt("db_user")
    OptionBuilder.withDescription("Database User")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("user")
    options.addOption(OptionBuilder.create("du"))

    OptionBuilder.withLongOpt("db_password")
    OptionBuilder.withDescription("Database Password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("password")
    options.addOption(OptionBuilder.create("dpw"))

    OptionBuilder.withLongOpt("db_name")
    OptionBuilder.withDescription("Database Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("database")
    options.addOption(OptionBuilder.create("dn"))

    OptionBuilder.withLongOpt("db_wtable")
    OptionBuilder.withDescription("Wiki Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablew")
    options.addOption(OptionBuilder.create("wt"))

    OptionBuilder.withLongOpt("db_itable")
    OptionBuilder.withDescription("inv Index Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablei")
    options.addOption(OptionBuilder.create("it"))

    /* Commands */
    val group = new OptionGroup()
    group.setRequired(false)

    OptionBuilder.withLongOpt("extract_Text")
    OptionBuilder.withDescription("Path to the XML-File containing the Wiki-Articles")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("wiki_file")
    group.addOption(OptionBuilder.create("e"))

    OptionBuilder.withLongOpt("index")
    OptionBuilder.withDescription("use db-entries to create an inverse index and stores it back")
    OptionBuilder.hasArgs(0)
    group.addOption(OptionBuilder.create("i"))

    OptionBuilder.withLongOpt("b_test")
    OptionBuilder.withDescription("benchmark test")
    OptionBuilder.hasArgs(0)
    options.addOption(OptionBuilder.create("bt"))

    options.addOptionGroup(group)
    options
  }

  def testPlagiarismFinder(finder: PlagiarismFinder) = {
    val input = raw"Korpiklaani (finn. „Klan der Wildnis“, auch „Klan des Waldes“) ist eine finnische Folk-Metal-Band aus Lahti mit starken Einflüssen aus der traditionellen Volksmusik. Die Texte der Band handeln von mythologischen Themen sowie der Natur und dem Feiern, wobei auch reine Instrumentalstücke in ihrem Repertoire enthalten sind. Sie selbst sehen ihre Musik auch vom Humppa beeinflusst. Bislang wurden sechs reguläre Studioalben und eine EP veröffentlicht, daneben eine Live-DVD, sowie eine Wiederveröffentlichung der Demos."
    val input2 = raw"Der Kragenbär, Asiatische Schwarzbär, Mondbär oder Tibetbär (Ursus thibetanus) ist eine Raubtierart aus der Familie der Bären (Ursidae). In seiner Heimat wird er meistens als black bear bezeichnet oder als Baribal. Im Vergleich zum eher gefürchteten Grizzlybär gilt der Schwarzbär als weniger gefährlich."

    //find plagiarisms using default hyper parameters
    val matches = finder.findPlagiarisms(input + input2, new HyperParameters())

    matches.foreach(println)
  }

  /*
   * Method that parses the wikidump and writes the articles in a cassandra dataspace.
   * Method for parsing the given Wikipedia-XML-Dump and wiritng its contents in a Cassandra Database. 
   * 
   * @param hadoopFile Filepath to the Wikipedia-XML Dump
   * @param cassandraPort Portnumber required to access Cassandra
   * @param cassandraDatabase IP required to access Cassandra
   * @param cassandraUser Cassandra Database User
   * @param cassandraPW Cassandra Database Password
   * @param  cassandraKeyspace Cassandra Keyspace (analog with MySQL Database) name 
   *  
   * If the given Cassandra Column Family doens't exist it will try to create it, note that this isnt optimal in cassandra.
   * Tables should be ALWAYS created beforehand to ensure efficient reads. If no table is created beforehand regardless spark will create one and
   * sample it's structure from the given Dataframe
   * 
   * 
   *  docID (Wikipedia documentID) [BIGINT]
   *  title (Wikipedia article title) [TEXT]
   *	wikitext (Wikipedia article text) [TEXT]
   * 
   *
   */
  private def extractTextCassandra(hadoopFile: String, cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, cassandraKeyspace: String, cassandraTables: String) {
    println("Import Wiki-Articles Cassandra")
    println("Starting Import")
    var appName = "WikiImporter_Cassandra"
    val sparkConf = SparkConfHelper.createCassandraSparkConf(appName, cassandraUser, cassandraPW, cassandraHost, cassandraPort.toString)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(hadoopFile)

    df.filter("ns = 0")
      .select("id", "title", "revision.text")
      .rdd.map(X => (X.getLong(0), X.getString(1), WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0))))
      .saveToCassandra(cassandraKeyspace, cassandraTables, SomeColumns("docid", "title", "wikitext"))
    println("Import Complete")
    sc.stop()
  }

  /*
   * Method that gets the wiki articles from the Wikipedia Articles Cassandra Column 
   * Family and then builds an inverse index for them and stores them in a new Column Family in a new Column Family 
   * in the Cassandra Database
   * 
   * @param cassandraPort Portnumber required to access Cassandra
   * @param cassandraDatabase IP required to access Cassandra
   * @param cassandraUser Cassandra Database User
   * @param cassandraPW Cassandra Database Password
   * @param cassandraKeyspace Cassandra Keyspace (analog with MySQL Database) name 
   * @param cassandraWikiTables Name of the Cassandra Column Family containing the Wikipedia-Articles
   * @param cassandraInvIndexTables Name of the Cassandra Column Family containing the inverse Indezes
   * 
   */
  private def createInverseIndexCass(cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, cassandraKeyspace: String, cassandraWikiTables: String, cassandraInvIndexTables: String, n: Int) {
    println("Creating InverseIndex for Cassandra")
    var appName = "[WIKIPLAG] Build Inverse Index WikipediaDE"
    val sparkConf = SparkConfHelper.createCassandraSparkConf(appName, cassandraUser, cassandraPW, cassandraHost, cassandraPort.toString)

    val sc = new SparkContext(sparkConf)

    val rdd = sc.cassandraTable(cassandraKeyspace, cassandraWikiTables)
    //tokenize and normalize the text
    val documents = rdd.map(x => (x.get[Long]("docid"), InverseIndexBuilderImpl.tokenizeAndNormalize(x.get[String]("wikitext"))))
    //build the inverse index x._1 is the docid, x._2 a list of words
    val invIndexEntries = documents.map(entry => InverseIndexBuilderImpl.buildInverseIndexNGram(n, entry._1, entry._2))
    //n-gram, docId,occurences
    //TODO remove hardcoded tuple stuff
    //TODO Some makes tuples
    val merge = invIndexEntries.flatMap(identity).map(x => (Tuple4(x._1.lift(0), x._1.lift(1), x._1.lift(2), x._1.lift(3)), x._2._1, x._2._2))

    merge.saveToCassandra(cassandraKeyspace, cassandraInvIndexTables, SomeColumns("ngram", "docid", "occurences"))
    sc.stop()
    println("Import Complete")
  }
}
