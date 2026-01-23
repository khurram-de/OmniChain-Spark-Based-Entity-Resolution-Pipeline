package omnichain.model

sealed trait Decision

object Decision {
  case object MATCHED extends Decision
  case object NOT_MATCHED extends Decision
}

sealed trait DecisionReason

object DecisionReason {
  case object ExactWalletMatch extends DecisionReason

  final case class AmountDeltaWithinTolerance(
      relativeDelta: Double,
      tolerance: Double
  ) extends DecisionReason
  final case class AmountDeltaAboveTolerance(
      relativeDelta: Double,
      tolerance: Double
  ) extends DecisionReason

  final case class NameSimilarityAboveThreshold(
      score: Double,
      threshold: Double
  ) extends DecisionReason
  final case class NameSimilarityBelowThreshold(
      score: Double,
      threshold: Double
  ) extends DecisionReason

  case object InsufficientEvidence extends DecisionReason
  case object ConflictingEvidence extends DecisionReason
  final case object TypeMatched extends DecisionReason
  final case object TypeMismatched extends DecisionReason

  final case class StepWithinWindow(
      distance: Long,
      maxAllowed: Long
  ) extends DecisionReason

  final case class StepOutsideWindow(
      distance: Long,
      maxAllowed: Long
  ) extends DecisionReason

}

final case class MatchDecision(
    decision: String, // "MATCHED" | "NOT_MATCHED"
    reasons: Seq[String],
    policyVersion: String
)
