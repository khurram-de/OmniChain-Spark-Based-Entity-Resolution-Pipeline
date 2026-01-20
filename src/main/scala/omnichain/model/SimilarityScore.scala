package omnichain.model

case class SimilarityScore(
    leftTxId: String,
    rightTxId: String,
    nameSimilarity: Double,
    walletExactMatch: Boolean,
    amountRelativeDifference: Double,
    blockKey: String
)
