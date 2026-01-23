package omnichain.model

final case class DecisionPolicy(
    policyVersion: String,
    nameSimilarityThreshold: Double,
    amountRelativeTolerance: Double,
    walletMatchEnabled: Boolean,
    requireTypeMatch: Boolean,
    maxStepDistance: Long,
    evidenceMode: EvidenceMode
)
