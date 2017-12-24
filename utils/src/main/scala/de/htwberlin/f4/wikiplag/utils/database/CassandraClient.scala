package de.htwberlin.f4.wikiplag.utils.database

import InverseIndexTable.NGram
import InverseIndexTable.DocId
import InverseIndexTable.Occurences
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.types.{TextType, TupleFieldDef, TupleType}
import org.apache.spark.SparkContext

/**
  * @author
  * Anton K.
  *
  * 09.12.2017
  *
  * A Cassandra Client for 4-Tuples
  * For more info about accessing cassandra tuples using the connector - https://docs.datastax.com/en/developer/java-driver/3.3/manual/tuples/
  */
class CassandraClient(sc: SparkContext, keyspace: String, inverseIndexTable: String, articlesTable: String) {

  //the cassandra tuple type. It must be the same as the one in the database
  val tupleType4 = new TupleType(TupleFieldDef(0, TextType), TupleFieldDef(1, TextType), TupleFieldDef(2, TextType), TupleFieldDef(3, TextType))

  /**
    * Retrieves all the 4-grams from the database using a single where in statement.
    *
    * @param ngrams The n-grams to look for
    * @return a cassandra rdd with the results
    */
  def query4Grams(ngrams: List[List[String]]): CassandraTableScanRDD[CassandraRow] = {
    if (ngrams == null || ngrams.isEmpty)
      throw new IllegalArgumentException("ngrams")
    val df = sc.cassandraTable(keyspace, inverseIndexTable)

    val ngramsAsCassandraTuplesList = ngrams.map(ngram => tupleType4.newInstance(ngram.headOption, ngram.lift(1), ngram.lift(2), ngram.lift(3)))
    val result = df.select(NGram, DocId, Occurences).where(NGram + " in ?", ngramsAsCassandraTuplesList)
    result
  }

  /**
    * Same as [[CassandraClient.query4Grams]] but returns an array of cassandra rows instead.
    **/
  def query4GramsAsArray(ngrams: List[List[String]]): Array[CassandraRow] = {
    query4Grams(ngrams).collect()
  }
}