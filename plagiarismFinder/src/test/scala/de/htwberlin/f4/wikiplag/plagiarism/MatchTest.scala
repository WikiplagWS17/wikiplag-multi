package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import org.junit.Test
import org.scalatest.FunSuite
import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import org.apache.spark.SparkContext
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class MatchTest extends AssertionsForJUnit {

  var finder: PlagiarismFinder = _
  var sc:SparkContext=_

  @Before def setUp() {
    var cassandraParameters = CassandraParameters.readFromConfigFile("app.conf")

    val sparkConf=cassandraParameters.toSparkConf("[Wikiplag] Plagiarism finder")
    sc = new SparkContext(sparkConf)
    val cassandraClient=new CassandraClient(sc,cassandraParameters)
    finder = new PlagiarismFinder(cassandraClient)
  }
  @After def tearDown(){
    sc.stop()
  }

  @Test def testPlagiarismFinderSentencePlagiate() {
    val input = raw"Ang Lee (|TEMPLATE|; * 23. Oktober 1954 in Pingtung, Taiwan) ist ein US-amerikanisch-taiwanischer Filmregisseur, Drehbuchautor und Produzent. Er ist als vielfach ausgezeichneter Regisseur bekannt für so unterschiedliche Filme wie Eat Drink Man Woman, die Jane-Austen-Adaption Sinn und Sinnlichkeit, den Martial Arts-Film Tiger and Dragon sowie Brokeback Mountain, für den er 2006 den Regie-Oscar erhielt. Einen weiteren Oscar erhielt er 2013 für seine Regiearbeit an Life of Pi: Schiffbruch mit Tiger.\n\n== Leben ==\nAng Lee wurde 1954 in Taiwan geboren. Seine Eltern, Emigranten aus China, lernten sich in Taiwan kennen, Lee ist ihr ältester Sohn. Die Großeltern väterlicher- und mütterlicherseits sind im Zuge der kommunistischen Revolution in China ums Leben gekommen. Da sein Vater als Lehrer häufiger die Arbeitsstelle wechselte, wuchs Ang Lee in verschiedenen Städten Taiwans auf.\n\nEntgegen den Wünschen seiner Eltern, wie sein Vater eine klassische akademische Laufbahn einzuschlagen, interessierte sich Lee für das Schauspiel und absolvierte mit ihrem Einverständnis zunächst ein Theater- und Filmstudium in Taipeh. Im Anschluss daran ging er 1978 in die USA, um an der Universität von Illinois in Urbana-Champaign Theaterwissenschaft und -regie zu studieren. Nach dem Erwerb seines B.A. in Illinois verlegte er sich ganz auf das Studium der Film- und Theaterproduktion an der Universität von New York, das er 1985 mit einem Master abschloss. Danach entschloss er sich, mit seiner ebenfalls aus Taiwan stammenden Ehefrau zusammen in den USA zu bleiben."
    val input2 = raw"Directors Guild of America Award ===\n* 1996: Nominierung in der Kategorie Beste Spielfilmregie für Sinn und Sinnlichkeit\n* 2001: Auszeichnung in der Kategorie Beste Spielfilmregie für Tiger & Dragon\n* 2006: Auszeichnung in der Kategorie Beste Spielfilmregie für Brokeback Mountain\n* 2013: Nominierung in der Kategorie Beste Spielfilmregie für Life of Pi: Schiffbruch mit Tiger\n\n=== Weitere Auszeichnungen ===\n* 1993: Goldener Bär der Berliner Filmfestspiele für Das Hochzeitsbankett\n* 1993: Golden Horse Beste Regie für Das Hochzeitsbankett\n* 1996: Goldener Bär der Berliner Filmfestspiele für Sinn und Sinnlichkeit\n* 1997: Bundesfilmpreis für den besten ausländischen Film mit Sinn und Sinnlichkeit\n* 2000: Golden Horse Bester Film für Tiger and Dragon\n* 2001: Hong Kong Film Award für Tiger and Dragon\n* 2002: Aufnahme in die American Academy of Arts and Sciences\n* 2005: Goldener Löwe des Filmfestivals in Venedig für Brokeback Mountain\n* 2007: Golden Horse Beste Regie für Gefahr und Begierde\n* 2007: Goldener Löwe des Filmfestivals in Venedig für Gefahr und Begierde\n\n== Siehe auch ==\n* Taiwanischer Film\n* US-amerikanischer Film\n\n== Literatur ==\n* Tanja Hanhart (Redaktorin): Ang Lee und sein Kino."
    //find plagiarisms using default hyper parameters
    val matches = finder.findPlagiarisms(input + input2, new HyperParameters())

    matches.foreach(println)
    matches.foreach(x => println("[" + (input + input2).substring(x._1.start, x._1.end) + "] Matches: " + x._2))
    assert(true)
  }

}
