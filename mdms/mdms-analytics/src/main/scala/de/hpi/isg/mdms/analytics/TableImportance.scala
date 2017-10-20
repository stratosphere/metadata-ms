package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.util.IdUtils
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

/**
  * @author Marius Walter
  *         Implementation of the  approach by Yang et al., 2009, to determine the table importances.
  * @param planBuilder
  */
class TableImportance()(implicit planBuilder: PlanBuilder) {
  /** Calculating the probability matrix, later used for calculating the table importance.
    * The approach is based on by Yang et al., 2009
    *
    * @param idUtils             utility tools
    * @param columnStatistics    is constraint collection column statistics
    * @param tupleCount          is constraint collection tuple count
    * @param inclusionDependency is constraint collection inclusion dependency
    * @param metadataStore       is the meta data store
    * @return the probability matrix
    */
  def probMatrix(idUtils: IdUtils,
                 columnStatistics: ConstraintCollection[ColumnStatistics],
                 tupleCount: ConstraintCollection[TupleCount],
                 inclusionDependency: ConstraintCollection[InclusionDependency],
                 metadataStore: MetadataStore
                ): DataQuanta[(Int, Int, Double)] = {
    // q ... total number of join edges
    // get the number of dependent columns
    val q1 = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
    // get the number of referenced columns
    val q2 = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getReferencedColumnIds.apply(0), 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
    val q = q1.union(q2)

    // qEnt ... product of q and entropy (for each column)
    val qEnt = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (cs.getColumnId, cs.getEntropy))
      .keyBy(_._1).join(q.keyBy(_._1)).assemble((a, b) => (b._1, a._2 * b._2))

    // qEntT ... product of q and entropy summed over the entire table
    val qEntT = qEnt
      .map(cs => (idUtils.getTableId(cs._1), cs._2))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))

    // numberTuples ... number of tuples of table
    val numberTuples = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId, tp.getNumTuples))

    // logRqEntT ... sum of log(numTuples) and qEntT
    val logRqEntT = qEntT
      .keyBy(_._1).join(numberTuples.keyBy(_._1)).assemble((a, b) => (a._1, a._2 + math.log(b._2)))

    // edgeCol ... Start and target of edge (COLUMN-ID used here)
    val edgeCol = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
      .flatMap(ind => (Seq(ind, ind.swap)))

    // Entropy of each column
    val entCol = metadataStore.loadConstraints(columnStatistics)
      .map(ind => (ind.getColumnId, ind.getEntropy))

    val entEdgeT = edgeCol
      .keyBy(_._1).join(entCol.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
      .map(a => (idUtils.getTableId(a._1), idUtils.getTableId(a._2), a._3))

    // probT ... probability of each table
    val probT = entEdgeT
      .keyBy(_._1).join(logRqEntT.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 / b._2))

    // Calculate the probability P[R,S]
    val probabilityRS = probT
      .filter(a => a._1 != a._2).reduceByKey(t => (t._1, t._2), (a, b) => (a._1, a._2, a._3 + b._3))

    // Getting dummy values for P[R,R]
    val dummyProbabilityRR = metadataStore.loadTables().map(tableMock => (tableMock.id, tableMock.id, 0.0))

    // Union between P[R,S] + (dummy) P[R,R]
    val dummyProbabilityRSRR = probabilityRS.union(dummyProbabilityRR)

    // Calculate final P[R,R]
    val probabilityRR = dummyProbabilityRSRR.reduceByKey(_._1, (a, b) => (a._1, a._2, a._3 + b._3))
      .map(a => (a._1, a._1, 1.0 - a._3))

    // Union between P[R,S] + P[R,R]
    val probability = probabilityRR.union(probabilityRS)

    // Return probability matrix
    return probability
  }

  /**
    * Finding the table importance based on the approach by Yang et al., 2009
    *
    * @param tupleCount       is the constraint collection tuple count
    * @param metadataStore    is the metadata store
    * @param idUtils          utility tools
    * @param columnStatistics is constraint collection column statistics
    * @param epsilon          if Euclidean Distance between V and V+1  is
    *                         lower epsilon, the stationary distribution is reached
    * @param maxNumIteration  maximum Number of Iterations
    * @return the solution vector containing the table importance
    */
  def tableImport(tupleCount: ConstraintCollection[TupleCount],
                  inclusionDependency: ConstraintCollection[InclusionDependency],
                  columnStatistics: ConstraintCollection[ColumnStatistics],
                  metadataStore: MetadataStore,
                  idUtils: IdUtils,
                  epsilon: Option[Double],
                  maxNumIteration: Option[Int])
  : DataQuanta[(Int, Double)] = {
    // Calculating the probability matrix
    val probabilityMatrix = probMatrix(idUtils, columnStatistics, tupleCount, inclusionDependency, metadataStore)
    // Setting up the initial solution vector
    val Vinitial = initiateVector(tupleCount, metadataStore, idUtils, columnStatistics)
    maxNumIteration match {
      case Some(maxNumIteration) => Vinitial.repeat(maxNumIteration, Vold => iteratingImportance(probabilityMatrix, Vold))
      case None => Vinitial.doWhile[Double](_.head < epsilon.getOrElse(1e-10), { Vold =>
        val Vnew = iteratingImportance(probabilityMatrix, Vold)
        val Vdiff = diffV(Vnew, Vold).map(a => math.sqrt(a))
        (Vnew, Vdiff.filter { x => println(x); true })})
    }
  }


  // Calculating the the product of the probability vector and the solution vector
  def iteratingImportance(probMatrix: DataQuanta[(Int, Int, Double)],
                          V: DataQuanta[(Int, Double)]): DataQuanta[(Int, Double)] = {
    return probMatrix
      .keyBy(a => a._1).join(V.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))
      .reduceByKey((_._2), (a, b) => (a._1, a._2, a._3 + b._3))
      .map(a => (a._2, a._3))
  }

  // Calculating the Euclidean Distance between the solution vector V and V+1
  def diffV(Vnew: DataQuanta[(Int, Double)], Vold: DataQuanta[(Int, Double)]): DataQuanta[(Double)] = {
    val diffVnewVold = Vnew.keyBy(_._1).join(Vold.keyBy(_._1)).assemble((a, b) => (a._2, b._2))
      .map(a => ((a._1 - a._2) * (a._1 - a._2))).reduce(_ + _)
    return diffVnewVold
  }

  // Initiating the solution vector
  def initiateVector(tupleCount: ConstraintCollection[TupleCount],
                     metadataStore: MetadataStore,
                     idUtils: IdUtils,
                     columnStatistics: ConstraintCollection[ColumnStatistics]): DataQuanta[(Int, Double)] = {
    // numberTuples ... number of tuples of table
    val numberTuples = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId, tp.getNumTuples))
    // entropy ... entropy for each table and summed up
    val entropy = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (idUtils.getTableId(cs.getColumnId), cs.getEntropy))
      .reduceByKey(_._1, (a, b) => (a._1, a._1 + a._2))
    // Combining R + entropy
    val Rentropy = numberTuples.keyBy(a => a._1).join(entropy.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
    //return Rentropy.map(a => (a._1, 1));
    return Rentropy.map(a => (a._1, a._2));
  }
}