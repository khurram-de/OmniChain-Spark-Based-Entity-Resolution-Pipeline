package omnichain.app

import org.apache.spark.sql.{SparkSession, Dataset}
import omnichain.transformations.SampleData.sampleTransactions
import omnichain.transformations.Blocking._
import omnichain.transformations.SimilarityScoring._
import omnichain.transformations.Decisioning._
import omnichain.model.{PairDecision, SimilarityScore}
import omnichain.transformations.generateCandidatePairs.{
  generateCandidatePairs,
  unionAndDeduplicate
}
import org.apache.spark.storage.StorageLevel
import omnichain.metrics.PipelineMetrics.{topBlockSizes, blockCapHitRate}
object Main {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Session
    // ------------------------------------------------------------
    val spark = SparkSession
      .builder()
      .appName("OmniChain")
      .master("local[*]")
      .getOrCreate()

    // ------------------------------------------------------------
    // Controls (keep in one place)
    // ------------------------------------------------------------
    val amountBlockCap = 10
    val topNBlocksToPrint = 10
    val SL = StorageLevel.MEMORY_AND_DISK

    // ------------------------------------------------------------
    // 1. Load / Generate Transactions
    // ------------------------------------------------------------
    val smpDS =
      sampleTransactions(spark)
        .persist(SL)

    println(s"Sampled transactions: ${smpDS.count()}")

    // ------------------------------------------------------------
    // 2. Blocking + Block Metrics
    // ------------------------------------------------------------
    val blockedByExactNameDS =
      blockingWithExactNameKey(smpDS)
        .persist(SL)

    topBlockSizes(
      "Exact Name Blocking",
      blockedByExactNameDS,
      topNBlocksToPrint
    ).show(false)

    val blockedByExactAmountDS =
      blockingWithAmountCentsKey(smpDS)
        .persist(SL)

    val blockedByAmountCentsDS =
      capBlocks(blockedByExactAmountDS, amountBlockCap)
        .persist(SL)

    // After both blocking passes are materialized, we no longer need the raw input cached
    smpDS.unpersist()

    topBlockSizes(
      "Amount Cents Blocking",
      blockedByAmountCentsDS,
      topNBlocksToPrint
    ).show(false)

    blockCapHitRate(
      "Capping Hit Rate",
      blockedByExactAmountDS,
      blockedByAmountCentsDS
    )

    // No longer needed after cap metrics
    blockedByExactAmountDS.unpersist()

    // ------------------------------------------------------------
    // 3. Candidate Pair Generation
    // ------------------------------------------------------------
    val candidatePairsByNameDS =
      generateCandidatePairs(blockedByExactNameDS)
        .persist(SL)

    println(
      s"Candidate pairs by Name Blocking: ${candidatePairsByNameDS.count()}"
    )

    // Safe to drop after candidate generation
    blockedByExactNameDS.unpersist()

    val candidatePairsByAmountDS =
      generateCandidatePairs(blockedByAmountCentsDS)
        .persist(SL)

    println(
      s"Candidate pairs by Amount Blocking: ${candidatePairsByAmountDS.count()}"
    )

    // Safe to drop after candidate generation
    blockedByAmountCentsDS.unpersist()

    // ------------------------------------------------------------
    // 4. Union + Deduplicate
    // ------------------------------------------------------------
    val allCandidatePairsDS =
      unionAndDeduplicate(candidatePairsByNameDS, candidatePairsByAmountDS)
        .persist(SL)

    candidatePairsByNameDS.unpersist()
    candidatePairsByAmountDS.unpersist()

    println(s"Total candidate pairs: ${allCandidatePairsDS.count()}")

    // ------------------------------------------------------------
    // 5. Similarity Scoring (next step)
    // ------------------------------------------------------------
    allCandidatePairsDS.unpersist()

    spark.stop()
  }
}
