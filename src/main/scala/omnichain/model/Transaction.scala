package omnichain.model

final case class Transaction(
    txId: String,
    name: String, // nameOrig
    wallet: String, // proxy: nameOrig (source account)
    amount: Double,
    txType: String, // PaySim `type`
    eventTime: Long // PaySim `step`
)
