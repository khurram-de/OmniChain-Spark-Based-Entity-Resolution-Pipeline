package omnichain.app

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{monotonically_increasing_id, desc}
import org.apache.spark.storage.StorageLevel

import omnichain.ingress.PaySimRaw
import omnichain.metrics.PipelineMetrics.{blockCapHitRate, topBlockSizes}
import omnichain.model.Transaction
import omnichain.transformations.Blocking._
import omnichain.transformations.Decisioning._
import omnichain.transformations.SimilarityScoring.toSimilarityScore
import omnichain.transformations.SampleData.sampleTransactions
import omnichain.transformations.generateCandidatePairs.{
  generateCandidatePairs,
  unionAndDeduplicate
}
import omnichain.metrics.PipelineMetrics
import omnichain.transformations.EntityResolution._
import omnichain.transformations.IntelligenceReport.topEntities
import omnichain.transformations.IntelligenceReport

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("OmniChain")
      .master("local[8]")
      // .config("spark.sql.shuffle.partitions", "200")
      // .config("spark.driver.memory", "12g")
      // .config("spark.driver.maxResultSize", "2g")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir(
      "tmp/omnichain-checkpoints"
    ) // Set checkpoint directory due to fsilkiness in connected components | EntityResolution

    import spark.implicits._

    println("============================================================")
    println(s"[BOOT] Spark Application Name: ${spark.sparkContext.appName}")
    println(s"[BOOT] Master: ${spark.sparkContext.master}")
    println(
      s"[BOOT] Default parallelism: ${spark.sparkContext.defaultParallelism}"
    )
    println("============================================================")

    val amountBlockCap = 500
    val topNBlocksToPrint = 20
    val SL = StorageLevel.MEMORY_AND_DISK_SER

    println(s"[CONFIG] StorageLevel: $SL")
    println(s"[CONFIG] amountBlockCap: $amountBlockCap")
    println(s"[CONFIG] topNBlocksToPrint: $topNBlocksToPrint")
    println("------------------------------------------------------------")

    val csvPath = "src/main/scala/omnichain/data/PaySim.csv"
    val parquetPath = "src/main/scala/omnichain/data/PaySim.parquet"

    val schema = PaySimRaw.getSchema()
    println(s"[INGRESS] Using PaySim schema:\n$schema")
    println("------------------------------------------------------------")

    // PaySim has no natural txId, so we synthesize one.
    val smpDS: Dataset[Transaction] =
      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(csvPath)
        // .repartition(400)
        .withColumn("txId", monotonically_increasing_id().cast("string"))
        .select(
          $"txId",
          $"nameOrig".as("name"),
          $"nameOrig".as("wallet"),
          $"amount",
          $"type".as("txType"),
          $"step".as("eventTime")
        )
        // .limit(6500000)
        .as[Transaction]

    // Alternate ingress (Parquet)
    // val smpDS: Dataset[Transaction] =
    //   spark.read
    //     .parquet(parquetPath)
    //     .as[Transaction]

    println(s"[INGRESS] Total transactions loaded: ${smpDS.count()}")
    println("------------------------------------------------------------")

    println("[BLOCKING] Pass A: Exact Name Blocking - START")
    val blockedByExactNameDS =
      blockingWithExactNameKey(smpDS)
    println("[BLOCKING] Pass A: Exact Name Blocking - DONE")
    println("------------------------------------------------------------")

    println("[METRICS] Pass A: Top blocks by Exact Name Blocking - START")
    val topBlocksByName = topBlockSizes(
      "Exact Name Blocking",
      blockedByExactNameDS.toDF("blockKey", "tx"),
      topNBlocksToPrint
    )
    println("[METRICS] Pass A: Top blocks by Exact Name Blocking - DONE")
    println("[METRICS] Pass A: Printing top blocks")
    topBlocksByName.show(false)
    println("------------------------------------------------------------")

    println("[BLOCKING] Pass B: Amount Cents Blocking - START")
    val blockedByExactAmountDS =
      blockingWithAmountCentsKey(smpDS)
    println("[BLOCKING] Pass B: Amount Cents Blocking - DONE")
    println("------------------------------------------------------------")

    println("[METRICS] Pass B: Top blocks by Amount Cents Blocking - START")
    topBlockSizes(
      "Amount Cents Blocking",
      blockedByExactAmountDS.toDF("blockKey", "tx"),
      topNBlocksToPrint
    ).show(false)
    println("[METRICS] Pass B: Top blocks by Amount Cents Blocking - DONE")
    println("------------------------------------------------------------")

    // Optional cap path
    println("[BLOCKING] Pass B: Applying capBlocks - START")
    val blockedByAmountCentsDS =
      capBlocks(blockedByExactAmountDS, amountBlockCap)
    // .persist(SL)
    println("[BLOCKING] Pass B: Applying capBlocks - DONE")

    println("[METRICS] Cap hit-rate - START")
    PipelineMetrics.blockCapHitRate(
      "Capping Hit Rate",
      blockedByExactAmountDS,
      blockedByAmountCentsDS
    )
    println("[METRICS] Cap hit-rate - DONE")

    blockedByExactAmountDS.unpersist()

    println("[CANDIDATES] Pass A: Candidate generation - START")
    val candidatePairsByNameDS =
      generateCandidatePairs(blockedByExactNameDS)
        .persist(SL)

    println(
      s"[CANDIDATES] Pass A: Candidate pairs by Name Blocking: ${candidatePairsByNameDS.count()}"
    )
    println("[CANDIDATES] Pass A: Candidate generation - DONE")
    println("------------------------------------------------------------")

    println(
      "[CACHE] Unpersist blockedByExactNameDS (safe after Pass A candidates materialized)"
    )
    blockedByExactNameDS.unpersist()
    println("------------------------------------------------------------")

    println("[CANDIDATES] Pass B: Candidate generation - START")
    val candidatePairsByAmountDS =
      generateCandidatePairs(blockedByAmountCentsDS)
        .persist(SL)

    println(
      s"[CANDIDATES] Pass B: Candidate pairs by Amount Blocking: ${candidatePairsByAmountDS.count()}"
    )
    println("[CANDIDATES] Pass B: Candidate generation - DONE")
    println("------------------------------------------------------------")

    println(
      "[CACHE] Unpersist blockedByExactAmountDS (safe after Pass B candidates materialized)"
    )
    blockedByExactAmountDS.unpersist()
    println("------------------------------------------------------------")

    println("[CANDIDATES] Union + Deduplicate - START")
    val allCandidatePairsDS =
      unionAndDeduplicate(candidatePairsByNameDS, candidatePairsByAmountDS)
        .persist(SL)

    candidatePairsByNameDS.unpersist()
    candidatePairsByAmountDS.unpersist()

    println(
      s"[CANDIDATES] Total candidate pairs: ${allCandidatePairsDS.count()}"
    )
    println("[CANDIDATES] Union + Deduplicate - DONE")
    println("------------------------------------------------------------")

    val similarityScoresDS =
      allCandidatePairsDS
        .map(toSimilarityScore)
        .persist(SL)

    println("[SCORING] Sample similarity scores:")
    similarityScoresDS.show(false)

    allCandidatePairsDS.unpersist()

    val policy = omnichain.config.PolicyLoader.load()
    println(s"[POLICY] Loaded decision policy: $policy")

    val decisionsDS =
      similarityScoresDS
        .map { score =>
          val decision = decide(score, policy)
          omnichain.model.PairDecision(
            score.leftTxId,
            score.rightTxId,
            score,
            decision.reasons,
            decision.decision,
            decision.policyVersion
          )
        }
        .persist(SL)

    val matchedDS = decisionsDS.filter($"decision" === "MATCHED")

    print(s"MATCHED COUNT: ${matchedDS.count()}\n")
    println("[DECISION] Sample decisions:")
    matchedDS.show(false)

    similarityScoresDS.unpersist()

    val matchedEdgesDF = matchedDS
      .select(
        $"leftTxId".as("src"),
        $"rightTxId".as("dst")
      )
      .filter($"src" =!= $"dst")
      .distinct()
      .persist(SL)
    decisionsDS.unpersist()

    println(
      s"[ENTITY RESOLUTION] Total matched edges for connected components: ${matchedEdgesDF.count()}\n"
    )
    println("[ENTITY RESOLUTION] Sample matched edges:")
    matchedEdgesDF.show(5, false)

    println("[ENTITY RESOLUTION] Resolving connected components - START")
    val resolvedEntitiesDF = resolveConnectedComponents(
      matchedEdgesDF,
      maxItrs = 5
    ).persist(SL)
    matchedEdgesDF.unpersist()

    println(
      s"[ENTITY RESOLUTION] Total resolved entities: ${resolvedEntitiesDF.count()}\n"
    )
    println("[ENTITY RESOLUTION] Sample resolved entities:")
    resolvedEntitiesDF.show(5, false)

    println("[ENTITY RESOLUTION] Resolved entity by sizes:")
    resolvedEntitiesDF
      .groupBy("resolvedEntityId")
      .count()
      .orderBy(desc("count"))
      .show(5, false)

    println("[ENTITY RESOLUTION] Resolving connected components - DONE")

    println("------------------------------------------------------------")

    println("[INTELLIGENCE REPORT] Generating Top Entities Report - START")
    val finalResolvedDF =
      IntelligenceReport.topEntities(
        transactions = smpDS,
        resolvedMapping = resolvedEntitiesDF,
        topN = 20
      )

    println("[INTELLIGENCE REPORT] Top Entities Report:")
    finalResolvedDF.show(false)
    println("[INTELLIGENCE REPORT] Generating Top Entities Report - DONE")
    println("------------------------------------------------------------")

    // cleanup
    resolvedEntitiesDF.unpersist()
    smpDS.unpersist()

    println("[SHUTDOWN] Stopping SparkSession")
    spark.stop()
    println("[SHUTDOWN] Done")
  }
}
