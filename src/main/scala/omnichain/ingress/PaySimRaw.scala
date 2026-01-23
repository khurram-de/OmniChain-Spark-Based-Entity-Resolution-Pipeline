package omnichain.ingress

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.StructType

final case class PaySimRaw(
    step: Long,
    `type`: String,
    amount: Double,
    nameOrig: String,
    oldbalanceOrg: Double,
    newbalanceOrig: Double,
    nameDest: String,
    oldbalanceDest: Double,
    newbalanceDest: Double,
    isFraud: Int,
    isFlaggedFraud: Int
)

object PaySimRaw {

  implicit val encoder: Encoder[PaySimRaw] =
    Encoders.product[PaySimRaw]

  def getSchema(): StructType =
    encoder.schema
}
