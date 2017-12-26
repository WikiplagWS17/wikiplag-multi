package de.htwberlin.f4.wikiplag.utils.database

import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable.NGram
import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable.DocId
import de.htwberlin.f4.wikiplag.utils.database.tables.InverseIndexTable.Occurrences
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import org.apache.spark.SparkContext

/**
  * Client for accessing the Cassandra database.
  */
class CassandraClient(sc: SparkContext, cassandraParameters: CassandraParameters) {

  /**
    * Same as [[CassandraClient.queryNGramHashes]] but returns an array of cassandra rows instead.
    **/
  def queryNGramHashesAsArray(ngramHashes: List[Long]): Array[CassandraRow] = {
    queryNGramHashes(ngramHashes).collect()
  }

  /**
    * Retrieves all the n-gram hashes from the database using a single where in statement.
    *
    * @param ngramhashes The n-grams to look for
    * @return a cassandra rdd with the results
    */
  def queryNGramHashes(ngramhashes: List[Long]): CassandraTableScanRDD[CassandraRow] = {
    if (ngramhashes == null || ngramhashes.isEmpty)
      throw new IllegalArgumentException("ngrams")
    val df = sc.cassandraTable(cassandraParameters.keyspace, cassandraParameters.inverseIndexTable)

    //val ngramsAsCassandraTuplesList = ngramhashes.map(ngram => )))
    val result = df.select(NGram, DocId, Occurrences).where(NGram + " in ?", ngramhashes)
    result
  }
}