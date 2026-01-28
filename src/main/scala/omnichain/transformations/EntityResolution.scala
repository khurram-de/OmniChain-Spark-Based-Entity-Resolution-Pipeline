package omnichain.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object EntityResolution {
  def resolveConnectedComponents(
      matchedEdges: DataFrame,
      maxItrs: Int = 10
  ): DataFrame = {
    /*
     * This function would implement an algorithm to find connected components
     */
    import matchedEdges.sparkSession.implicits._
    // val revrseMatchEdges = matchedEdges.select(
    //   $"dst".as("src"),
    //   $"src".as("dst")
    // )

    val edges = matchedEdges
      .select($"src", $"dst")
      .union(matchedEdges.select($"dst".as("src"), $"src".as("dst")))
      .filter($"src" =!= $"dst")
      .distinct()
      .persist()

    val nodes = edges
      .select($"src".as("txId"))
      .union(edges.select($"dst".as("txId")))
      .distinct()
      .persist()

    var labels = nodes
      .select($"txId", $"txId".as("label"))
      .persist()

    var iter = 0L
    var changed = Long.MaxValue

    while (maxItrs > iter && changed > 0) {

      val propagated = edges
        .join(labels, edges("src") === labels("txId"), "inner")
        .select(edges("dst").as("txId"), labels("label").as("candLabel"))

      val bestCand = propagated
        .groupBy($"txId")
        .agg(min($"candLabel").as("bestLabel"))

      val newLabels = labels
        .join(bestCand, Seq("txId"), "left")
        .select(
          $"txId",
          least($"label", coalesce($"bestLabel", $"label")).as("label")
        )
        .persist()
      //   val newLabelsCp = newLabels.checkpoint(eager = true) // using localCheckpoint due to Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
      val newLabelsCp = newLabels.localCheckpoint(eager =
        true
      ) // check point due to spark lineage issues in connected components

      newLabels.unpersist()

      val oldL = labels.as("old")
      val newL = newLabelsCp.as("new")

      changed = newL
        .join(oldL, Seq("txId"))
        .filter($"new.label" =!= $"old.label")
        .count()

      labels.unpersist()
      labels = newLabelsCp

      //   val oldL = labels.as("old")
      //   val newL = newLabels.as("new")

      //   changed = newL
      //     .join(oldL, Seq("txId"))
      //     .filter($"new.label" =!= $"old.label")
      //     .count()

      //   labels.unpersist()
      //   labels = newLabels

      iter += 1
      println(s"[ENTITY RESOLUTION] iter=$iter changed=$changed")

      // Update labels in labledUndirectedEdges
      // This is a placeholder; actual implementation would require more logic
    }

    edges.unpersist()

    // output mapping
    labels.select($"txId", $"label".as("resolvedEntityId"))

  }
}
