package omnichain.model
/*
 * Case class to represent candidate pairs of transactions for record linkage
 * - left: The first transaction in the candidate pair
 * - right: The second transaction in the candidate pair
 * - blockKey: The blocking key used to generate this candidate pair
 */

case class CandidatePairs(
    left: Transaction,
    right: Transaction,
    blockKey: String
)

final case class CandidatePairKeyed(
    key: String,
    candidatePair: CandidatePairs
)
