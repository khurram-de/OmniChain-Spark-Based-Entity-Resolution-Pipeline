package omnichain.transformations

/* Decisioning logic for entity resolution */
import omnichain.model.{
  DecisionPolicy,
  SimilarityScore,
  DecisionReason,
  MatchDecision,
  EvidenceMode
}
import omnichain.model.DecisionReason._

object Decisioning {

  /** Evidence extraction: convert raw scores into typed, explainable reasons
    * (facts).
    */
  def reasonsFromScore(
      score: SimilarityScore,
      policy: DecisionPolicy
  ): Set[DecisionReason] = {

    val walletReasons: Set[DecisionReason] =
      if (policy.walletMatchEnabled && score.walletExactMatch)
        Set(ExactWalletMatch)
      else Set.empty

    val nameSimilarityReasons: Set[DecisionReason] =
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

    val amountDeltaReasons: Set[DecisionReason] =
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

    val typeReasons: Set[DecisionReason] =
      Set(if (score.typeExactMatch) TypeMatched else TypeMismatched)

    val stepReasons: Set[DecisionReason] =
      Set(
        if (score.timeDistance <= policy.maxStepDistance)
          StepWithinWindow(score.timeDistance, policy.maxStepDistance)
        else
          StepOutsideWindow(score.timeDistance, policy.maxStepDistance)
      )

    val reasons: Set[DecisionReason] =
      walletReasons ++ nameSimilarityReasons ++ amountDeltaReasons ++ stepReasons ++ typeReasons

    // Tag InsufficientEvidence using the SAME sufficiency logic as decisioning (policy-driven).
    if (!hasStrongEvidence(reasons, policy)) reasons + InsufficientEvidence
    else reasons
  }

  /** Policy-driven sufficiency: what counts as "strong evidence" under a given
    * dataset mode.
    */
  def hasStrongEvidence(
      reasons: Set[DecisionReason],
      policy: DecisionPolicy
  ): Boolean = {

    val identifierStrong: Boolean =
      reasons.exists {
        case ExactWalletMatch                   => true
        case NameSimilarityAboveThreshold(_, _) => true
        case _                                  => false
      }

    val amountWithinTol: Boolean =
      reasons.exists {
        case AmountDeltaWithinTolerance(_, _) => true; case _ => false
      }

    val stepWithin: Boolean =
      reasons.exists { case StepWithinWindow(_, _) => true; case _ => false }

    val typeMatched: Boolean =
      reasons.exists { case TypeMatched => true; case _ => false }

    val behavioralStrong: Boolean =
      amountWithinTol && stepWithin && (!policy.requireTypeMatch || typeMatched)

    policy.evidenceMode match {
      case EvidenceMode.IDENTIFIER => identifierStrong
      case EvidenceMode.BEHAVIORAL => behavioralStrong
    }
  }

  /** Policy interpretation: apply gates and required evidence to produce a
    * MatchDecision.
    */
  def decideFromReasons(
      reasons: Set[DecisionReason],
      policy: DecisionPolicy
  ): MatchDecision = {

    val typeMismatchPresent: Boolean =
      reasons.exists { case TypeMismatched => true; case _ => false }

    val stepOutsidePresent: Boolean =
      reasons.exists { case StepOutsideWindow(_, _) => true; case _ => false }

    val blockedByType: Boolean =
      policy.requireTypeMatch && typeMismatchPresent

    val blockedByStep: Boolean =
      stepOutsidePresent

    // Same strong-evidence logic as reasonsFromScore; decisioning must not drift.
    val strongEvidencePresent: Boolean =
      hasStrongEvidence(reasons, policy)

    val blockedByInsufficientEvidence: Boolean =
      !strongEvidencePresent

    // Required evidence for PaySim behavioral matching.
    val amountWithinTolPresent: Boolean =
      reasons.exists {
        case AmountDeltaWithinTolerance(_, _) => true; case _ => false
      }

    val stepWithinPresent: Boolean =
      reasons.exists { case StepWithinWindow(_, _) => true; case _ => false }

    val typeMatchedPresent: Boolean =
      reasons.exists { case TypeMatched => true; case _ => false }

    val hasRequiredEvidence: Boolean =
      amountWithinTolPresent &&
        stepWithinPresent &&
        (!policy.requireTypeMatch || typeMatchedPresent)

    val matchStatus: Boolean =
      !blockedByType &&
        !blockedByStep &&
        !blockedByInsufficientEvidence &&
        hasRequiredEvidence

    val finalDecision: String =
      if (matchStatus) "MATCHED" else "NOT_MATCHED"

    val reasonStrings: Seq[String] =
      reasons.toSeq.map(_.toString).sorted

    MatchDecision(finalDecision, reasonStrings, policy.policyVersion)
  }

  /** Thin wrapper: score -> reasons -> decision. */
  def decide(
      score: SimilarityScore,
      policy: DecisionPolicy
  ): MatchDecision =
    decideFromReasons(reasonsFromScore(score, policy), policy)
}
