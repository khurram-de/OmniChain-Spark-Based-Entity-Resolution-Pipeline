package omnichain.model

case class DecisionPolicy(
    policyVersion: String,
    nameSimilarityThreshold: Double,
    amountRelativeTolerance: Double
)
