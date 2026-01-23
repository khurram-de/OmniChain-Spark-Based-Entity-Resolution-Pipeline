package omnichain.model

case class SimilarityScore(
    leftTxId: String,
    rightTxId: String,
    nameSimilarity: Double,
    walletExactMatch: Boolean,
    amountRelativeDifference: Double,
    blockKey: String,
    typeExactMatch: Boolean, // New field for behavioral type exact match
    timeDistance: Long // new field for behavioral time distance in seconds
)
