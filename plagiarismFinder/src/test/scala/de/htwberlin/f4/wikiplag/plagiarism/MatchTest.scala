package de.htwberlin.f4.wikiplag.plagiarism

import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import org.junit.Test
import org.scalatest.FunSuite
import de.htwberlin.f4.wikiplag.plagiarism.models.HyperParameters
import de.htwberlin.f4.wikiplag.utils.CassandraParameters
import de.htwberlin.f4.wikiplag.utils.database.CassandraClient
import junit.framework.Assert
import junit.framework.Assert._
import org.apache.spark.SparkContext
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit
import org.hamcrest.CoreMatchers
import org.junit.Assert

class MatchTest extends AssertionsForJUnit {

  var finder: PlagiarismFinder = _
  var sc: SparkContext = _

  @Before def setUp() {
    var cassandraParameters = CassandraParameters.readFromConfigFile("app.conf")

    val sparkConf = cassandraParameters.toSparkConf("[Wikiplag] Plagiarism finder")
    sc = new SparkContext(sparkConf)
    val cassandraClient = new CassandraClient(sc, cassandraParameters)
    finder = new PlagiarismFinder(cassandraClient)
  }

  @After def tearDown() {
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

  @Test def testPlagiarismFinderPartOfSentencePlagiate() {
    val input = raw"\n\n; Konsistenz\n: Ein Kalkül wird genau dann konsistent genannt, wenn es unmöglich ist, mit Hilfe seiner Axiome und Regeln einen Widerspruch herzuleiten, d.\xa0h. eine Aussage der Form P ∧ ¬ P (z.\xa0B. „Hugo ist groß, und Hugo ist nicht groß“). Für einen Kalkül, der in der Aussagenlogik verwendet werden soll, ist das eine Mindestanforderung.\n: Ist es in einem Kalkül möglich, einen Widerspruch herzuleiten, dann wird der Kalkül inkonsistent genannt.\n: Es gibt formale Systeme, in denen solch ein Widerspruch hergeleitet werden kann, die aber durchaus sinnvoll sind. Für solche Systeme wird ein anderer Konsistenzbegriff verwendet: Ein Kalkül ist konsistent, wenn in ihm nicht alle Formeln herleitbar sind (siehe parakonsistente Logik).\n: Es lässt sich leicht zeigen, dass für die klassische Logik die beiden Konsistenzbegriffe zusammenfallen: In der klassischen Logik lässt sich aus einem Widerspruch jeder beliebige Satz herleiten (dieser Sachverhalt wird Ex falso quodlibet genannt), d.\xa0h. wenn ein klassischer Kalkül auch nur einen Widerspruch herleiten könnte, also im ersten Sinn inkonsistent wäre, dann könnte er jede Aussage herleiten, wäre also im zweiten Sinn inkonsistent. Wenn umgekehrt ein Kalkül inkonsistent im zweiten Sinn ist, also in ihm jede Aussage herleitbar ist, dann ist insbesondere auch jeder Widerspruch herleitbar und ist er auch inkonsistent im ersten Sinn.\n\n; Korrektheit\n: Ein Kalkül heißt genau dann korrekt (semantisch korrekt), wenn in ihm nur solche Formeln hergeleitet werden können, die auch semantisch gültig sind. Für die klassische Aussagenlogik bedeutet das einfacher: Ein Kalkül ist genau dann korrekt, wenn in ihm nur Tautologien bewiesen und nur gültige Argumente hergeleitet werden können.\n: Ist es in einem aussagenlogischen Kalkül möglich, mindestens ein ungültiges Argument herzuleiten oder mindestens eine Formel zu beweisen, die keine Tautologie ist, dann ist der Kalkül inkorrekt.\n\n; Vollständigkeit\n: Vollständig (semantisch vollständig) heißt ein Kalkül genau dann, wenn in ihm alle semantisch gültigen Formeln hergeleitet werden können; für die klassische Aussagenlogik: Wenn in ihm alle Tautologien hergeleitet werden können.\n\n; Adäquatheit\n: Ein Kalkül heißt genau dann im Hinblick auf eine spezielle Semantik adäquat, wenn er (semantisch) korrekt und (semantisch) vollständig ist.\n\nEin metatheoretisches Resultat ist zum Beispiel die Feststellung, dass alle korrekten Kalküle auch konsistent sind. Ein anderes metatheoretisches Resultat ist die Feststellung, dass ein konsistenter Kalkül nicht automatisch korrekt sein muss: Es ist ohne weiteres möglich, einen Kalkül aufzustellen, in dem zwar kein Widerspruch hergeleitet werden kann, in dem aber z.\xa0B. die nicht allgemeingültige Aussage der Form „A ∨ B“ hergeleitet werden kann. Ein solcher Kalkül wäre aus ersterem Grund konsistent, aus letzterem Grund aber nicht korrekt.\n\nEin weiteres, sehr einfaches Resultat ist die Feststellung, dass ein vollständiger Kalkül nicht automatisch auch korrekt oder nur konsistent sein muss. Das einfachste Beispiel wäre ein Kalkül, in dem jede Formel der aussagenlogischen Sprache herleitbar ist. Da jede Formel herleitbar ist, sind alle Tautologien herleitbar, die ja Formeln sind: Das macht den Kalkül vollständig. Da aber jede Formel herleitbar ist, ist insbesondere auch die Formel P0 ∧ ¬ P0 und die Formel A ∨ B herleitbar: Ersteres macht den Kalkül inkonsistent, letzteres inkorrekt.\n\nDas Ideal, das ein Kalkül erfüllen sollte, ist Korrektheit und Vollständigkeit: Wenn das der Fall ist, dann ist er der ideale Kalkül für ein logisches System, weil er alle semantisch gültigen Sätze (und nur diese) herleiten kann. So sind die beiden Fragen, ob ein konkreter Kalkül korrekt und/oder vollständig ist und ob es für ein bestimmtes logisches System überhaupt möglich ist, einen korrekten und vollständigen Kalkül anzugeben, zwei besonders wichtige metatheoretische Fragestellungen.\n\n== Abgrenzung und Philosophie ==\n\nDie klassische Aussagenlogik, wie sie hier ausgeführt wurde, ist ein formales logisches System. Als solches ist sie eines unter vielen, die aus formaler Sicht gleichwertig nebeneinander stehen und die ganz bestimmte Eigenschaften haben: Die meisten sind konsistent, die meisten sind korrekt, etliche sind vollständig, und einige sind sogar entscheidbar. Aus formaler Sicht stehen die logischen Systeme in keinem Konkurrenzverhalten hinsichtlich Wahrheit oder Richtigkeit.\n\nVon formalen, innerlogischen Fragen klar unterschieden sind außerlogische Fragen: Solche nach der Nützlichkeit (Anwendbarkeit) einzelner Systeme für einen bestimmten Zweck und solche nach dem philosophischen, speziell metaphysischen Status einzelner Systeme.\n\nDie Nützlichkeitserwägung ist die einfachere, bezüglich deren Meinungsunterschiede weniger tiefgehend bzw. weniger schwerwiegend sind. Klassische Aussagenlogik zum Beispiel bewährt sich in der Beschreibung elektronischer Schaltungen (Schaltalgebra) oder zur Formulierung und Vereinfachung logischer Ausdrücke in Programmiersprachen. Prädikatenlogik wird gerne angewandt, wenn es darum geht, Faktenwissen zu formalisieren und automatisiert Schlüsse daraus zu ziehen, wie das unter anderem im Rahmen der Programmiersprache Prolog geschieht. Fuzzy-Logiken, nonmonotone, mehrwertige und auch parakonsistente Logiken sind hochwillkommen, wenn es darum geht, mit Wissensbeständen umzugehen, in denen Aussagen mit unterschiedlich starkem Gewissheitsgrad oder gar einander widersprechende Aussagen abgelegt werden sollen und dennoch sinnvolle Schlüsse aus dem Gesamtbestand gezogen werden sollen. Auch wenn es je nach Anwendungsfall sehr große Meinungsunterschiede geben kann, welches logisches System besser geeignet ist, ist die Natur des Problems für alle Beteiligten unmittelbar und in gleicher Weise greifbar. Einzelwissenschaftliche Überlegungen und Fragestellungen spielen sich überwiegend in diesem Bereich ab.\n\n(Noch) kontroverser als solche pragmatischen Überlegungen sind Fragestellungen philosophischer und metaphysischer Natur. Geradezu paradigmatisch ist die Frage, „welches logische System richtig ist“, wobei „richtig“ hier gemeint ist als: Welches logische System nicht nur einen Teilaspekt der Wirklichkeit modellhaft vereinfacht, sondern die Wirklichkeit, das Sein als Ganzes adäquat beschreibt. Zu dieser Fragestellung gibt es viele unterschiedliche Meinungen einschließlich der vom philosophischen Positivismus eingeführten Meinung, dass die Fragestellung als Ganz"
    val input2 = raw"Prevailed sincerity behaviour to so do principle mr. As departure at no propriety zealously my. On dear rent if girl view. First on smart there he sense. Earnestly enjoyment her you resources. Brother chamber ten old against. Mr be cottage so related minuter is. Delicate say and blessing ladyship exertion few margaret. Delight herself welcome against smiling its for. Suspected discovery by he affection household of principle perfectly he."
    val matches = finder.findPlagiarisms(input + input2, new HyperParameters())
    val str1 = matches.foreach(println)
    //matches.foreach(x => println("[" + (input).substring(x._1.start, x._1.end) + "]"))
    val str2 = str1.toString
    assertFalse(str2.contains("Prevailed"))
    assertFalse(str2.contains("principle"))
    assertFalse(str2.contains("zealously"))

  }


  @Test def testPlagiarismFinderSentenceNotPlagiate() {
    val input = raw"Jeremy Braddock, Stephen Hock Jeremy Braddock, Stephen Hock (Hrsg.): Directed by Allen Smithee. Foreword by Andrew Sarris."
    val input2 = raw"Die hatte den mit laut Börsenwerte. Oliver verwendet erfreuten drohte mit, und vermittelte Frau können war geben. Sie Mann Ausdruck Frau sie suspendiert, sah nicht an dem Gemeinde Armenhaus. Oh verhaftet Software erst ihr, das alles er bietet und. Und erfahrungsgemäß Fenster ein schwache noch Andeutungen eigentlich Arzt Stadt Annehmlichkeiten. Der niemals wurden müssen durch Verfahren, man mitlachen sie berühmtes sie Auftrag der sicher Personen, die sozialen Gut damit können Ich durch. Besonders Begriff stand von hatte einigen vor, wenn auf zu Namens offenbar brachte ungewöhnlichen Wächtern betrachtete, tatsächlich wäe und Ausstrecken greisenhafter hierbleiben erklärte herrschte Weise bekämen. Besonders aber die."
    val matches = finder.findPlagiarisms(input + input2, new HyperParameters())
    val str1 = matches.foreach(println)
    val str2 = str1.toString
    assertEquals(str2, "()")

  }

}
