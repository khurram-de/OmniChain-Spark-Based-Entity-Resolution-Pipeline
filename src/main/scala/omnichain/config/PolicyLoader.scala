package omnichain.config

import com.typesafe.config.ConfigFactory
import omnichain.model.DecisionPolicy
import omnichain.model.EvidenceMode

object PolicyLoader {

  def load(): DecisionPolicy = {
    val config = ConfigFactory.load("application.conf")

    val path = "omnichain.policy"

    val version = config.getString(s"$path.version")
    val nameThreshold = config.getDouble(s"$path.nameSimilarityThreshold")
    val amountTolerance = config.getDouble(s"$path.amountRelativeTolerance")
    val walletMatchEnabled = config.getBoolean(s"$path.walletMatchEnabled")
    val evidenceMode =
      EvidenceMode.fromString(config.getString(s"$path.evidenceMode"))

    val decisionPolicy = DecisionPolicy(
      policyVersion = version,
      nameSimilarityThreshold = nameThreshold,
      amountRelativeTolerance = amountTolerance,
      walletMatchEnabled = walletMatchEnabled,
      requireTypeMatch = config.getBoolean(s"$path.requireTypeMatch"),
      maxStepDistance = config.getLong(s"$path.maxStepDistance"),
      evidenceMode = evidenceMode
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

    require(
      policy.maxStepDistance >= 0,
      s"maxStepDistance must be >= 0, but got ${policy.maxStepDistance}"
    )
  }
}
