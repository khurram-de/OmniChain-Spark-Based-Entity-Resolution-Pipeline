package omnichain.app

import org.apache.spark.sql.{SparkSession, Dataset}

// Sample data generator with intentional identity gaps
import omnichain.transformations.SampleData.sampleTransactions

// Blocking logic to reduce O(n^2) comparisons
import omnichain.transformations.Blocking.withBlockingKeys

// Candidate pair generation within blocks
import omnichain.transformations.generateCandidatePairs.generateCandidatePairs

// Similarity scoring logic (pure functions, Dataset-native)
import omnichain.transformations.SimilarityScoring._

import omnichain.transformations.Decisioning.decide

// Final flattened decision output (Spark-friendly schema)
import omnichain.model.{PairDecision, SimilarityScore}

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
      .getOrCreate()

    // Required for Dataset encoders
    import spark.implicits._

    // ------------------------------------------------------------
    // 1. Generate Sample Transactions
    // ------------------------------------------------------------
    // Creates a Dataset[Transaction] with intentional identity gaps
    // (e.g. name variants, multiple wallets for same person)
    val smpDS = sampleTransactions(spark)
    println(s"Sampled transactions: ${smpDS.count()}")

    // ------------------------------------------------------------
    // 2. Blocking
    // ------------------------------------------------------------
    // Add blocking keys to reduce candidate comparisons.
    // This avoids O(n^2) explosion in entity resolution.
    val blocked = withBlockingKeys(smpDS)

    // ------------------------------------------------------------
    // 3. Candidate Pair Generation
    // ------------------------------------------------------------
    // Generate candidate pairs only within the same block.
    // Cached because we both count and later transform it.
    val candidatePairs = generateCandidatePairs(blocked).cache()
    println(s"Generated candidate pairs: ${candidatePairs.count()}")

    // ------------------------------------------------------------
    // 4. Similarity Scoring
    // ------------------------------------------------------------
    // Convert each candidate pair into a similarity score.
    // Uses pure Scala functions (no UDFs).
    val scored: Dataset[SimilarityScore] =
      candidatePairs.map(toSimilarityScore).cache()
    println(s"Scored candidate pairs: ${scored.count()}")

    // ------------------------------------------------------------
    // 5. Load Decision Policy (Driver-only)
    // ------------------------------------------------------------
    // Policy is loaded once on the driver, validated, and then
    // safely captured in Spark closures.
    val policy = omnichain.config.PolicyLoader.load()
    println(s"Loaded decision policy: $policy")

    // ------------------------------------------------------------
    // 6. Decisioning
    // ------------------------------------------------------------
    // Apply decision logic to each scored pair.
    //
    // IMPORTANT DESIGN CHOICE:
    // - Internally we use rich ADTs (DecisionReason, etc.)
    // - At the Spark boundary we emit only primitive, encodable types
    //   (String, Seq[String]) for schema stability and explainability.
    val matchDecisions: Dataset[PairDecision] =
      scored
        .map { score =>
          val md = decide(score, policy)
          PairDecision(
            score.leftTxId,
            score.rightTxId,
            score.blockKey,
            md.decision, // "MATCHED" | "NOT_MATCHED"
            md.reasons, // Seq[String]
            md.policyVersion
          )
        }
        .cache()

    println(s"Generated match decisions: ${matchDecisions.count()}")

    // ------------------------------------------------------------
    // 7. Output (Demo)
    // ------------------------------------------------------------
    // Show final resolved decisions.
    // In a real pipeline this would be written to storage.
    matchDecisions.show(50, truncate = false)

    // ------------------------------------------------------------
    // Shutdown
    // ------------------------------------------------------------
    spark.stop()
  }
}
