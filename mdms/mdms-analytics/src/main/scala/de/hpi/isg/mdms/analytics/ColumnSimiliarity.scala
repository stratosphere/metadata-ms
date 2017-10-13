package de.hpi.isg.mdms
package analytics


import de.hpi.isg.mdms.domain.constraints.{Vector => QGramSketch}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.targets.{Column, Target}
import org.qcri.rheem.api.{DataQuanta, PlanBuilder, _}

/**
  * This utility provides means to calculate the similarity/distance of [[Column]]s.
  */
object ColumnSimiliarity {

  /**
    * Calculate the pairwise distance of [[Column]]s within via Q-gram sketches.
    *
    * @param mds           the [[MetadataStore]] that hosts the schemata and constraints
    * @param qGramSketches the q-gram sketches for the `scope`
    * @param scope         [[Target]]s that entail a set of [[Column]]s (see [[collectColumns()]])
    * @param planBuilder   to set up the calculation with Rheem
    * @return the pairwise [[ColumnDistance]]s between [[Column]]s for which there was a `qGramSketches` provided
    *         a Q-gram sketch; only pairs of [[Column]]s are contained where the left [[Column]] has the smaller ID
    */
  def calculateDistancesWithQGramSketches(mds: MetadataStore,
                                          qGramSketches: ConstraintCollection[QGramSketch],
                                          scope: Target*)
                                         (implicit planBuilder: PlanBuilder):
  DataQuanta[ColumnDistance] = {
    val columnIds = collectColumns(scope: _*).map(_.getId)
    val sketches = mds.loadConstraints[QGramSketch](qGramSketches).withName("Load Q-gram sketches")
      .filter(sketch => columnIds.contains(sketch.getColumnId))
      .withCardinalityEstimator(columnIds.size)

    sketches.cartesian(sketches.map(x => x).withName("Work around"))
      .filter(pair => pair.field0.getColumnId < pair.field1.getColumnId, selectivity = 0.5)
      .withName("Filter redundant pairs")

      .map { pair =>
        val (sketch1, sketch2) = (pair.field0, pair.field1)
        var sum = 0d
        for (i <- sketch1.getValues.indices) {
          val delta = sketch1.getValues.apply(i) - sketch2.getValues.apply(i)
          sum += delta * delta
        }
        ColumnDistance(sketch1.getColumnId, sketch2.getColumnId, sum / math.sqrt(sketch1.getValues.length))
      }.withName("")
  }

}

/**
  * Stores the similarity of two [[Column]]s.
  */
case class ColumnDistance(columnId1: Int, columnId2: Int, value: Double)