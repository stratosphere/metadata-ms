package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.util.IdUtils
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}


class TableSimilarity()(implicit planBuilder: PlanBuilder) {

  def getFanout(tupleCount: ConstraintCollection[TupleCount],
                metadataStore: MetadataStore
               ): DataQuanta[(Int, Double)] = {
    val numberTuples = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId, tp.getNumTuples.toDouble))

    return numberTuples
  }

  def getStrength(idUtils: IdUtils,
                  columnStatistics: ConstraintCollection[ColumnStatistics],
                  tupleCount: ConstraintCollection[TupleCount],
                  inclusionDependency: ConstraintCollection[InclusionDependency],
                  metadataStore: MetadataStore,
                  matchingFraction: DataQuanta[(Int, Double)],
                  fanout: DataQuanta[(Int, Double)]
                 ): DataQuanta[(Int, Int, Double)] = {

    val strength1 = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
      .flatMap(ind => (Seq(ind, ind.swap)))
      .keyBy(_._1).keyJoin(matchingFraction.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
      .keyBy(_._2).keyJoin(matchingFraction.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))

    val strength2 = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
      .flatMap(ind => (Seq(ind, ind.swap)))
      .keyBy(_._1).keyJoin(fanout.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
      .keyBy(_._2).keyJoin(fanout.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))

    val strength = strength1
      .keyBy(_._1).keyJoin(strength2.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 / b._3))
      .map(x => (idUtils.getTableId(x._1), idUtils.getTableId(x._2), x._3))
      .reduce((a, b) => if (a._2 > b._2) a else b)

    val dummyStrength = metadataStore.loadTables().map(tableMock => (tableMock.id, tableMock.id, 1.0))

    return strength.union(dummyStrength)
  }

  def distance(strength: DataQuanta[(Int, Int, Double)])
  : DataQuanta[(Int, Int, Double)] = {

    return strength.map(x => (x._1, x._2, 1.0 - x._3))
  }
}


