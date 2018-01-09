package de.htwberlin.f4.wikiplag.plagiarism.models


/**
  * @author
  * Anton K.
  *
  * Encapsulates the hyper parameters.
  * @param minimumSentenceLength    The minimum length (number of words) in a sentence.
  *                                 Shorter sentences are merged together to be atleast this long.
  * @param threshold                The initial threshold.
  *                                 Articles which have an unique n-grams to input n-grams ratio
  *                                 below this threshold are filtered out
  * @param maxDistanceBetweenNgrams The maximum distance between two n-grams.
  *                                 If the distance of 2 given n-grams is above this
  *                                 they are split into separate plagiarism segments
  * @param maxAverageDistance       The maximum average distance between a potential plagiarism segment.
  * @param secondaryThreshold       The secondary threshold - applied after building plagiarism segments
  *                                 similarly to [[HyperParameters.threshold]]
  **/
class HyperParameters(val minimumSentenceLength: Int = 12, val threshold: Float = 0.25f,
                      val maxDistanceBetweenNgrams: Int = 6, val maxAverageDistance: Int = 4,
                      val secondaryThreshold: Float = 0.25f) {

  override def toString: String = s"HyperParameters(minimumSentenceLength=$minimumSentenceLength," +
    s" threshold=$threshold, maxDistanceBetweenNgrams=$maxDistanceBetweenNgrams," +
    s" maxAverageDistance=$maxAverageDistance, secondaryThreshold=$secondaryThreshold)"
}