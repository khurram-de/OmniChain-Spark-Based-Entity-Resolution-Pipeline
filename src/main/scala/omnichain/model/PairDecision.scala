package omnichain.model

case class PairDecision(
    leftTxId: String,
    rightTxId: String,
    similarityScore: SimilarityScore,
    decisionReasons: Seq[String],
    decision: String,
    policyVersion: String
)
