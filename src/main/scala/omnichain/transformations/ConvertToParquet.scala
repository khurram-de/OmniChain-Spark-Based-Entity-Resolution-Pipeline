package omnichain.transformations
import org.apache.spark.sql.{SparkSession, Dataset}
import omnichain.ingress.PaySimRaw
import org.apache.spark.sql.functions.monotonically_increasing_id

object ConvertToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CSV to Parquet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.read
      .option("header", "true")
      .schema(PaySimRaw.getSchema())
      .csv("src/main/scala/omnichain/data/PaySim.csv")
      .withColumn("txId", monotonically_increasing_id().cast("string"))
      .select(
        $"txId",
        $"nameOrig".as("name"),
        $"nameOrig".as("wallet"),
        $"amount",
        $"type".as("txType"),
        $"step".as("eventTime")
      )
      .write
      .mode("overwrite")
      .parquet("src/main/scala/omnichain/data/PaySim.parquet")

    println("Conversion complete!")
    spark.stop()
  }
}
