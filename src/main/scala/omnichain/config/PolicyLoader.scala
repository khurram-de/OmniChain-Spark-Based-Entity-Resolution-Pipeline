package omnichain.config

import com.typesafe.config.ConfigFactory
import omnichain.model.DecisionPolicy

object PolicyLoader {

  def load(): DecisionPolicy = {
    val config = ConfigFactory.load("application.conf")

    val path = "omnichain.policy"

    val version = config.getString(s"$path.version")
    val nameThreshold = config.getDouble(s"$path.nameSimilarityThreshold")
    val amountTolerance = config.getDouble(s"$path.amountRelativeTolerance")

    val decisionPolicy = DecisionPolicy(
      policyVersion = version,
      nameSimilarityThreshold = nameThreshold,
      amountRelativeTolerance = amountTolerance
    )

    validatePolicy(decisionPolicy)
    decisionPolicy
  }

  private def validatePolicy(policy: DecisionPolicy): Unit = {
    require(
      policy.nameSimilarityThreshold >= 0.0 && policy.nameSimilarityThreshold <= 1.0,
      s"Name similarity threshold must be between 0.0 and 1.0, but got ${policy.nameSimilarityThreshold}"
    )

    require(
      policy.amountRelativeTolerance >= 0.0 && policy.amountRelativeTolerance <= 1.0,
      s"Amount relative tolerance must be between 0.0 and 1.0, but got ${policy.amountRelativeTolerance}"
    )

    require(
      policy.policyVersion.nonEmpty,
      "Policy version must be a non-empty string"
    )
  }
}
