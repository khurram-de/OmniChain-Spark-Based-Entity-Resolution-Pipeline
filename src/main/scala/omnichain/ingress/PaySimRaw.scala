package omnichain.ingress

final case class PaySimRaw(
    step: Long,
    `type`: String,
    amount: Double,
    nameOrig: String,
    oldbalanceOrg: Double,
    newbalanceOrig: Double,
    nameDest: String,
    oldbalanceDest: Double,
    newbalanceDest: Double,
    isFraud: Int,
    isFlaggedFraud: Int
)
