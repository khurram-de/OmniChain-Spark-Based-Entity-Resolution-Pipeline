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
    // blocked
    //   .groupByKey(_._1)
    //   .flatMapGroups {
    //     (blockKey: String, records: Iterator[(String, Transaction)]) =>
    //       val txs = records.map(_._2).toSeq
    //       for {
    //         i <- txs.indices
    //         j <- (i + 1) until txs.length
    //       } yield CandidatePairs(
    //         left = txs(i),
    //         right = txs(j),
    //         blockKey = blockKey
    //       )
    //   }
    // flatMapGroups approach was too much memory intensive for large blocks.
    // Using a self-join approach instead.
    blocked
      .toDF("blockKey", "tx")
      .join(
        blocked.toDF("blockKey2", "tx2"),
        expr("blockKey = blockKey2 AND tx.txId < tx2.txId")
      )
      .select(
        struct(col("tx.*")).as("left"),
        struct(col("tx2.*")).as("right"),
        col("blockKey").as("blockKey")
      )
      .as[CandidatePairs]

  }

  def unionAndDeduplicate(
      ds1: Dataset[CandidatePairs],
      ds2: Dataset[CandidatePairs]
  ): Dataset[CandidatePairs] = {
    /*
     * Union two datasets of CandidatePairs and deduplicate them
     * - Deduplication is based on the combination of left and right transaction IDs
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
