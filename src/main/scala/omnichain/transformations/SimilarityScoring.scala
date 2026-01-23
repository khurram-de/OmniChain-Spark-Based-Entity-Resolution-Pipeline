package omnichain.transformations

import omnichain.transformations.Blocking.normalizeName
import omnichain.model._

object SimilarityScoring {

  private def normalizeType(t: String): String =
    t.trim.toUpperCase

  def nameSimilarityScore(
      leftName: String,
      rightName: String
  ): Double = {
    /*
     * Calculating name similarity using Levenshtein distance
     * - Normalizing both names before comparison
     * - Using dynamic programming to compute the distance
     * - Similarity score is calculated as 1 - (distance / max length of the two names)
     */
    val normalizedLeft = normalizeName(leftName)
    val normalizedRight = normalizeName(rightName)
    val m = normalizedLeft.length
    val n = normalizedRight.length
    val mtx: Array[Array[Int]] = Array.ofDim[Int](m + 1, n + 1)
    for (i <- 0 to m) mtx(i)(0) = i
    for (j <- 0 to n) mtx(0)(j) = j

    for (i <- 1 to m) {
      for (j <- 1 to n) {
        val cost =
          if (normalizedLeft(i - 1) == normalizedRight(j - 1)) 0 else 1
        mtx(i)(j) = minOf3(
          mtx(i - 1)(j) + 1, // deletion
          mtx(i)(j - 1) + 1, // insertion
          mtx(i - 1)(j - 1) + cost // substitution / match
        )
      }
    }
    val maxLen = math.max(m, n)
    val distance = mtx(m)(n)
    val similarity =
      if (maxLen == 0) 1.0
      else 1.0 - distance.toDouble / maxLen.toDouble

    f"$similarity%.2f".toDouble

  }

  def minOf3(
      a: Int,
      b: Int,
      c: Int
  ): Int = {
    /*
     * Utility function to find minimum of three integers
     */
    math.min(a, math.min(b, c))
  }

  def walletExactMatch(
      walletLeft: String,
      walletRight: String
  ): Boolean = {
    /*
     * Check if two wallet addresses match exactly (case insensitive)
     */
    if (walletLeft.trim.toLowerCase == walletRight.trim.toLowerCase) true
    else false
  }

  def amountRelativeDelta(
      amountLeft: Double,
      amountRight: Double
  ): Double = {
    val numerator = math.abs(amountLeft - amountRight)
    val denominator = (math.abs(amountLeft) + math.abs(amountRight)) / 2.0

    val delta =
      if (denominator == 0.0) 0.0
      else numerator / denominator

    f"$delta%.2f".toDouble
  }

  def toSimilarityScore(
      pair: CandidatePairs
  ): SimilarityScore = {
    /*
     * Convert a CandidatePairs instance to SimilarityScore
     * - Computes name similarity, wallet exact match, and amount difference
     * - Returns a SimilarityScore instance encapsulating these metrics
     */
    val typeExactMatch =
      normalizeType(pair.left.txType) == normalizeType(pair.right.txType)
    val timeDistance =
      math.abs(pair.left.eventTime - pair.right.eventTime) // in seconds
    SimilarityScore(
      pair.left.txId,
      pair.right.txId,
      nameSimilarityScore(pair.left.name, pair.right.name),
      walletExactMatch(pair.left.wallet, pair.right.wallet),
      amountRelativeDelta(pair.left.amount, pair.right.amount),
      pair.blockKey,
      typeExactMatch, // type exact match
      timeDistance // step distance (PaySim step units)
    )
  }
}
