package omnichain.transformations
/* Decisioning logic for entity resolution */
import omnichain.model.DecisionPolicy
import omnichain.model.SimilarityScore
import omnichain.model.DecisionReason
import omnichain.model.DecisionReason._
import omnichain.model.Decision
import omnichain.model.MatchDecision
import omnichain.model.Decision._

object Decisioning {
  def reasonsFromScore(
      score: SimilarityScore,
      policy: DecisionPolicy
  ): Set[DecisionReason] = {

    val base = Set.empty[DecisionReason]

    val walletReasons =
      if (score.walletExactMatch) Set(ExactWalletMatch)
      else Set.empty[DecisionReason]

    val nameSimilarityReasons =
      if (score.nameSimilarity >= policy.nameSimilarityThreshold)
        Set(
          NameSimilarityAboveThreshold(
            score.nameSimilarity,
            policy.nameSimilarityThreshold
          )
        )
      else
        Set(
          NameSimilarityBelowThreshold(
            score.nameSimilarity,
            policy.nameSimilarityThreshold
          )
        )

    val amountDeltaReasons =
      if (score.amountRelativeDifference >= policy.amountRelativeTolerance)
        Set(
          AmountDeltaAboveTolerance(
            score.amountRelativeDifference,
            policy.amountRelativeTolerance
          )
        )
      else
        Set(
          AmountDeltaWithinTolerance(
            score.amountRelativeDifference,
            policy.amountRelativeTolerance
          )
        )

    val reasonsList =
      base ++ walletReasons ++ nameSimilarityReasons ++ amountDeltaReasons

    val hasStrongEvidence =
      reasonsList.exists {
        case ExactWalletMatch                   => true
        case NameSimilarityAboveThreshold(_, _) => true
        case _                                  => false
      }

    if (!hasStrongEvidence) reasonsList ++ Set(InsufficientEvidence)
    else reasonsList

  }

  def decideFromReasons(
      reasons: Set[DecisionReason],
      policy: DecisionPolicy
  ): MatchDecision = {

    val matchStatus =
      reasons.exists {
        case ExactWalletMatch                   => true
        case NameSimilarityAboveThreshold(_, _) => true
        case _                                  => false
      }

    val finalDecision =
      if (matchStatus) "MATCHED" else "NOT_MATCHED"

    val reasonStrings: Seq[String] =
      reasons.toSeq.map(_.toString).sorted

    MatchDecision(finalDecision, reasonStrings, policy.policyVersion)
  }

  def decide(
      score: SimilarityScore,
      policy: DecisionPolicy
  ): MatchDecision = {
    decideFromReasons(
      reasonsFromScore(score, policy),
      policy
    )
  }
}
