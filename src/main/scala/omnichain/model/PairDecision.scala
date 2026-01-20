package omnichain.model

final case class PairDecision(
    leftTxId: String,
    rightTxId: String,
    blockKey: String,
    decision: String, // "MATCHED" | "NOT_MATCHED"
    reasons: Seq[String],
    policyVersion: String
)
