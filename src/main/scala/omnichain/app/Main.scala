package omnichain.app

import org.apache.spark.sql.{SparkSession, Dataset}

// Sample data generator with intentional identity gaps
import omnichain.transformations.SampleData.sampleTransactions

// Blocking logic to reduce O(n^2) comparisons
import omnichain.transformations.Blocking._

// Candidate pair generation within blocks
import omnichain.transformations.generateCandidatePairs._

// Similarity scoring logic (pure functions, Dataset-native)
import omnichain.transformations.SimilarityScoring._

import omnichain.transformations.Decisioning._

// Final flattened decision output (Spark-friendly schema)
import omnichain.model.{PairDecision, SimilarityScore}

import omnichain.transformations.generateCandidatePairs.{
  generateCandidatePairs,
  unionAndDeduplicate
}
import javassist.tools.reflect.Sample

object Main {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Session
    // ------------------------------------------------------------
    // For local development we explicitly set master.
    // In real deployments, this is controlled by spark-submit.
    val spark = SparkSession
      .builder()
      .appName("OmniChain")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .config("spark.sql.shuffle.partitions", "64")
      .config("spark.default.parallelism", "64")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .getOrCreate()

    // Required for Dataset encoders
    import spark.implicits._

    // ------------------------------------------------------------
    // 1. Generate Sample Transactions
    // ------------------------------------------------------------
    // Creates a Dataset[Transaction] with intentional identity gaps
    // (e.g. name variants, multiple wallets for same person)
    // val smpDS = sampleTransactions(spark)
    // println(s"Sampled transactions: ${smpDS.count()}")

    val smpDS = sampleTransactions(spark)
    // Alternatively, load real data from CSV
    //   omnichain.ingress.DataLoader
    //     .loadPaySimDataTxns(spark, "src/main/scala/omnichain/data/PaySim.csv")
    println(s"PaySim transactions: ${smpDS.count()}")

    // ------------------------------------------------------------
    // 2. Blocking
    // ------------------------------------------------------------
    val backedByExactNameDS =
      blockingWithExactNameKey(smpDS)
    val backedByAmountCentsDS =
      capBlocks(blockingWithAmountCentsKey(smpDS), 100)

    // ------------------------------------------------------------
    // 3. Generate Candidate Pairs
    // ------------------------------------------------------------
    val candidatePairsByNameDS =
      generateCandidatePairs(backedByExactNameDS)
    println(
      s"Candidate pairs by Name Blocking: ${candidatePairsByNameDS.count()}"
    )
    val candidatePairsByAmountDS =
      generateCandidatePairs(backedByAmountCentsDS)
    println(
      s"Candidate pairs by Amount Blocking: ${candidatePairsByAmountDS.count()}"
    )

    println(
      s"Union raw pairs: ${candidatePairsByNameDS.union(candidatePairsByAmountDS).count()}"
    )

    // ------------------------------------------------------------
    // 4. Union and Deduplicate Candidate Pairs from different blocking strategies
    // ------------------------------------------------------------
    val allCandidatePairsDS =
      unionAndDeduplicate(
        candidatePairsByNameDS,
        candidatePairsByAmountDS
      )

    println(s"Total candidate pairs: ${allCandidatePairsDS.count()}")
    // ------------------------------------------------------------
    // 5. Similarity Scoring
    // ------------------------------------------------------------

    spark.stop()
  }
}
