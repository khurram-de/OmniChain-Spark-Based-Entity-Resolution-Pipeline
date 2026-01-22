package omnichain.transformations
/* Generate candidate pairs from blocked transactions */
import org.apache.spark.sql.{Dataset, SparkSession}
import omnichain.model.{CandidatePairs, Transaction}
import org.apache.spark.sql.functions._

object generateCandidatePairs {
  def generateCandidatePairs(
      blocked: Dataset[(String, Transaction)]
  ): Dataset[CandidatePairs] = {
    import blocked.sparkSession.implicits._
    blocked
      .groupByKey(_._1)
      .flatMapGroups {
        (blockKey: String, records: Iterator[(String, Transaction)]) =>
          val txs = records.map(_._2).toSeq
          for {
            i <- txs.indices
            j <- (i + 1) until txs.length
          } yield CandidatePairs(
            left = txs(i),
            right = txs(j),
            blockKey = blockKey
          )
      }

  }

  def unionAndDeduplicate(
      ds1: Dataset[CandidatePairs],
      ds2: Dataset[CandidatePairs]
  ): Dataset[CandidatePairs] = {
    /*
     * Placeholder for union and deduplication logic of candidate pairs from different blocking strategies
     */
    import ds1.sparkSession.implicits._
    ds1
      .union(ds2)
      .map { pair =>
        val a = pair.left.txId
        val b = pair.right.txId
        val key = if (a <= b) s"$a|$b" else s"$b|$a"
        (key, pair)
      }
      .dropDuplicates("_1")
      .map(_._2)

  }
}
