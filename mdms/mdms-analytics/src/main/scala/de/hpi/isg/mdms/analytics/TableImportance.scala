package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.ColumnStatistics
import de.hpi.isg.mdms.domain.constraints.InclusionDependency
import de.hpi.isg.mdms.domain.constraints.TupleCount
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.util.IdUtils
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.java.Java
import de.hpi.isg.mdms.analytics


class TableImportance() {

  implicit val planBuilder = new PlanBuilder(new RheemContext().withPlugin(Java.basicPlugin))

  def probMatrix(idUtils: IdUtils,
                 columnStatistics: ConstraintCollection[ColumnStatistics],
                 tupleCount: ConstraintCollection[TupleCount],
                 inclusionDependency: ConstraintCollection[InclusionDependency],
                 metadataStore: MetadataStore
                ): DataQuanta[(Int, Int, Double)] = {


    // q ... total number of join edges
    val q = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))

    // qEnt ... product of q and entropy (for each column)
    val qEnt = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (cs.getColumnId, cs.getEntropy))
      .keyBy(a => a._1).keyJoin(q.keyBy(_._1)).assemble((a, b) => (b._1, a._2 * b._2))

    // qEntT ... product of q and entropy summed over the entire table
    val qEntT = qEnt
      .map(cs => (idUtils.getTableId(cs._1), cs._2))
      .reduceByKey(_._2, (a, b) => (a._1, a._2 + b._2))

    // R ... number of tuples of table
    val R = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId, tp.getNumTuples))

    // logRqEntT ... sum of log(numTuples) and qEntT
    val logRqEntT = qEntT
      .keyBy(a => a._1).keyJoin(R.keyBy(_._1)).assemble((a, b) => (b._1, a._2 + math.log(b._2)))

    // edgeCol ... Start and target of edge (COLUMN-ID used here)
    val edgeCol = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))

    // Entropy of each column
    val entCol = metadataStore.loadConstraints(columnStatistics)
      .map(ind => (ind.getColumnId, ind.getEntropy))

    // Entropy of each edge (stored with Tablr ID)
    val entEdgeTab = entCol.keyBy(a => a._1).keyJoin(edgeCol.keyBy(_._1))
      .assemble((a, b) => (b._1, a._2)).distinct
      .map(ind => (idUtils.getTableId(ind._1), ind._2))

    // probCol ... probability of each Column
    val probCol = entEdgeTab
      .keyBy(a => a._1).keyJoin(logRqEntT.keyBy(_._1))
      .assemble((a, b) => (a._1, a._2 / b._2))

    // Create tuple with [Start, Target, Probability]
    val probColEdge = edgeCol.map(ind => (idUtils.getTableId(ind._1), idUtils.getTableId(ind._2)))
      .keyBy(a => a._1).keyJoin(probCol.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))

    // Calculate the probability P[R,S]
    val probabilityRS = probColEdge
      .filter(a => a._1 != a._2).reduceByKey(t => (t._1, t._2), (a, b) => (a._1, a._2, a._3 + b._3))

    // Getting dummy values for P[R,R]
    val dummyProbabilityRR = metadataStore.loadTables().map(tableMock => (tableMock.id, tableMock.id, 0.0))

    // Union between P[R,S] + (dummy) P[R,R]
    val dummyProbabilityRSRR = probabilityRS.union(dummyProbabilityRR)

    // Calculate final P[R,R]
    val probabilityRR = dummyProbabilityRSRR.reduceByKey(_._1, (a, b) => (a._1, a._2, a._3 + b._3))
      .map(a => (a._1, a._1, 1 - a._3))

    // Union between P[R,S] + P[R,R]
    val probability = probabilityRR.union(probabilityRS)

    return probability
  }

  def tableImport(probMatrix: DataQuanta[(Int, Int, Double)],
                  tupleCount: ConstraintCollection[TupleCount],
                  metadataStore: MetadataStore,
                  idUtils: IdUtils,
                  columnStatistics: ConstraintCollection[ColumnStatistics],
                  epsilon: Double = 1e-10,
                  numIteration: Int = 100,
                  maxnumOrEpsilon: Boolean = true)
  : DataQuanta[(Int, Double)] = {
    var Vnew = initiateVector(tupleCount, metadataStore, idUtils, columnStatistics)
    var Vold = Vnew.map(a => (a._1, a._2))

    // Looping until reaching chosen max number of iteration
    if (maxnumOrEpsilon) {
      Vnew = Vold.repeat(numIteration, Vold => iteratingImportance(probMatrix, Vold))
    }
    // Looping until changes of vectors is below a critical threshold
    else {
      Vnew = Vold.doWhile[Double](_.head < epsilon, { Vold =>
        var Vnew = iteratingImportance(probMatrix, Vold)
        var Vdiff = diffV(Vnew, Vold).map(a => math.sqrt(a))
        (Vnew, Vdiff)
      })
    }

    //Vnew.collect().foreach(println)

    return Vnew
  }

  def iteratingImportance(probMatrix: DataQuanta[(Int, Int, Double)],
                        V: DataQuanta[(Int, Double)])
                        : DataQuanta[(Int, Double)] = {
    return probMatrix
      .keyBy(a => a._1).keyJoin(V.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))
      .reduceByKey((_._2), (a, b) => (a._1, a._2, a._3 + b._3))
      .map(a => (a._1, a._3))
  }

  def diffV(Vnew: DataQuanta[(Int, Double)], Vold: DataQuanta[(Int, Double)]): DataQuanta[(Double)] = {
    val diffVnewVold = Vnew.keyBy(_._1).keyJoin(Vold.keyBy(_._1)).assemble((a, b) => (a._2, b._2))
      .map(a => ((a._1 - a._2) * (a._1 - a._2))).reduce(_ + _)
    return diffVnewVold
  }

  def initiateVector(tupleCount: ConstraintCollection[TupleCount],
                     metadataStore: MetadataStore,
                     idUtils: IdUtils,
                     columnStatistics: ConstraintCollection[ColumnStatistics]): DataQuanta[(Int, Double)] = {
    //implicit val planBuilder = new PlanBuilder(new RheemContext().withPlugin(Java.basicPlugin))
    // R ... number of tuples of table
    val R = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId, tp.getNumTuples))
    // entropy ... entropy for each table and summed up
    val entropy = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (idUtils.getTableId(cs.getColumnId), cs.getEntropy))
      .reduceByKey(_._1, (a, b) => (a._1, a._1 + a._2))
    // Combining R + entropy
    val Rentropy = R.keyBy(a => a._1).keyJoin(entropy.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
    // Creating solution vector V
    return Rentropy.map(a => (a._1, math.log(a._2) + a._3))
  }
}

