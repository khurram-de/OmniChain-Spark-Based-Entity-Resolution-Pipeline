package omnichain.ingress

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{concat_ws, sha2}
import omnichain.model.Transaction

object DataLoader {
  private val paySimSchema: StructType =
    StructType(
      Seq(
        StructField("step", LongType, nullable = false),
        StructField("type", StringType, nullable = false),
        StructField("amount", DoubleType, nullable = false),
        StructField("nameOrig", StringType, nullable = false),
        StructField("oldbalanceOrg", DoubleType, nullable = false),
        StructField("newbalanceOrig", DoubleType, nullable = false),
        StructField("nameDest", StringType, nullable = false),
        StructField("oldbalanceDest", DoubleType, nullable = false),
        StructField("newbalanceDest", DoubleType, nullable = false),
        StructField("isFraud", IntegerType, nullable = false),
        StructField("isFlaggedFraud", IntegerType, nullable = false)
      )
    )

  def loadPaySimDataTxns(
      spark: SparkSession,
      path: String
  ): Dataset[Transaction] = {
    import spark.implicits._
    val rawDF = spark.read
      .option("header", true)
      .option("mode", "FAILFAST")
      .schema(paySimSchema)
      .csv(path)

    // ---
    val txDS = rawDF
      .withColumn(
        "txId",
        sha2(
          concat_ws(
            "-",
            $"step",
            $"type",
            $"nameOrig",
            $"nameDest",
            $"amount"
          ),
          256
        )
      )
      .select(
        $"txId",
        $"nameOrig".as("name"),
        $"nameOrig".as("wallet"),
        $"amount",
        $"type".as("txType"),
        $"step".as("eventTime")
      )
      .as[Transaction]

    txDS

  }

}
