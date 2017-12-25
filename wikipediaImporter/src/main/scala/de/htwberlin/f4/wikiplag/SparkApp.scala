package de.htwberlin.f4.wikiplag

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import de.htwberlin.f4.wikiplag.utils.database.tables.{ArticlesTable, InverseIndexTable, TokenizedTable}
import de.htwberlin.f4.wikiplag.utils.{CassandraParameters, InverseIndexBuilderImpl}
import de.htwberlin.f4.wikiplag.utils.parser.WikiDumpParser

/**
  * TODO docs
  **/
object SparkApp {

  def main(args: Array[String]) {

    val result = CommandLineOptions.parse(args)
    val cassandraParameters = result._1
    val action = result._2
    val actionParameter = result._3

    if (action == CommandLineOptions.Commands.ExtractWikiText) {
      extractWikitextAndStoreInCassandra(actionParameter, cassandraParameters)
    }

    else if (action == CommandLineOptions.Commands.TokenizeWikiText) {
      tokenizeAndStoreInCassandra(cassandraParameters)
    }

    else if (action == CommandLineOptions.Commands.BuildInverseIndex) {
      createInverseIndexAndStoreInCassandra(Integer.parseInt(actionParameter), cassandraParameters)
    }
    else
      throw new IllegalArgumentException("Unknown action: " + action + ". Did you add a new option forget to check it here?")
  }

  /**
    * Parses the Wikipedia-XML Dump, removing wiki markup and html and writes the articles to a cassandra table.
    *
    * Note:
    * If the given Cassandra table doesn't exist it will try to create it by sampling its structure from the
    * dataframe. This isn't optimal in cassandra, tables should ALWAYS be created beforehand to ensure efficient reads.
    *
    * Table structure:
    * [[ArticlesTable.DocId]] (Wikipedia documentID) [BIGINT]
    * [[ArticlesTable.Title]](Wikipedia article title) [TEXT]
    * [[ArticlesTable.WikiText]](Wikipedia article text) [TEXT]
    *
    * @param hadoopFile          Filepath to the Wikipedia-XML Dump
    * @param cassandraParameters The cassandra parameters
    *
    */
  private def extractWikitextAndStoreInCassandra(hadoopFile: String, cassandraParameters: CassandraParameters) {
    println("[WIKIPLAG] Import wiki-articles ")
    var appName = "WikiImporter_Cassandra"
    val sparkConf = cassandraParameters.toSparkConf(appName)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(hadoopFile)

    df.filter("ns = 0")
      .select("id", "title", "revision.text")
      .rdd.map(X => (X.getLong(0).toInt, X.getString(1), WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0))))
      .saveToCassandra(cassandraParameters.keyspace, cassandraParameters.articlesTable, SomeColumns(ArticlesTable.DocId, ArticlesTable.Title, ArticlesTable.WikiText))
    println("Import Complete")
    sc.stop()
  }

  /**
    * Tokenizes the already parsed Wikipedia text, removing and stores it to a cassandra table.
    *
    * Note:
    * If the given cassandra table doesn't exist it will try to create it by sampling its structure from the
    * dataframe. This isn't optimal in cassandra, tables should ALWAYS be created beforehand to ensure efficient reads.
    *
    * @param cassandraParameters The cassandra parameters
    *
    */
  private def tokenizeAndStoreInCassandra(cassandraParameters: CassandraParameters) {
    println("Tokenizing wikipedia articles.")

    var appName = "[WIKIPLAG] Tokenize WikipediaDE"
    val sparkConf = cassandraParameters.toSparkConf(appName)
    val sc = new SparkContext(sparkConf)

    val rdd = sc.cassandraTable(cassandraParameters.keyspace, cassandraParameters.articlesTable)

    //tokenize and normalize the text of each article
    val documents = rdd.map(x => (x.get[Int](ArticlesTable.DocId), InverseIndexBuilderImpl.tokenizeAndNormalize(x.get[String](ArticlesTable.WikiText))))

    //save it to the cassandra table
    documents.saveToCassandra(cassandraParameters.keyspace, cassandraParameters.tokenizedTable, SomeColumns(TokenizedTable.DocId, TokenizedTable.Tokens))
    sc.stop()
    println("Tokenizing done.")
  }

  /**
    * Creates the inverse index of the aldready tokenized wikitext.
    *
    * Note:
    * If the given cassandra table doesn't exist it will try to create it by sampling its structure from the
    * dataframe. This isn't optimal in cassandra, tables should ALWAYS be created beforehand to ensure efficient reads.
    *
    * @param n                   the n for the n-grams
    * @param cassandraParameters The cassandra parameters
    *
    *
    */
  private def createInverseIndexAndStoreInCassandra(n: Int, cassandraParameters: CassandraParameters) {
    println("Creating inverse index for Cassandra")
    var appName = "[WIKIPLAG] Build inverse index WikipediaDE"
    val sparkConf = cassandraParameters.toSparkConf(appName)

    val sc = new SparkContext(sparkConf)

    val rdd = sc.cassandraTable(cassandraParameters.keyspace, cassandraParameters.tokenizedTable)
    //get the tokenize and normalized text from the database
    val documents = rdd.map(x => (x.get[Int](TokenizedTable.DocId), x.get[List[String]](TokenizedTable.Tokens)))

    //build the inverse index
    val invIndexEntries = documents.map(entry => InverseIndexBuilderImpl.buildInverseIndexNGramHashes(n, entry._1, entry._2))

    //n-gram hash, docId, occurrences
    val merge = invIndexEntries.flatMap(identity).map(x => (x._1, x._2._1, x._2._2))
    merge.saveToCassandra(cassandraParameters.keyspace, cassandraParameters.inverseIndexTable,
      SomeColumns(InverseIndexTable.NGram, InverseIndexTable.DocId, InverseIndexTable.Occurrences))
    sc.stop()
    println("Import Complete")
  }
}

