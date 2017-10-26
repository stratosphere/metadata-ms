package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

/**
  * Implementation of the table importance algorithm described in
  * <p>Summarizing relational databases. <i>Yang et al.</i>, Proceedings of the VLDB Endowment 2(1), 2009.</p>
  *
  * @author Marius Walter
  */
object TableImportance {

  /** Calculating the probability matrix, later used for calculating the table importance.
    *
    * @param columnStatistics is constraint collection column statistics
    * @param tupleCounts      is constraint collection tuple count
    * @param foreignKeys      foreign keys of the schema
    * @param metadataStore    is the meta data store
    * @return the probability matrix
    */
  def tableTransitionMatrix(columnStatistics: ConstraintCollection[ColumnStatistics],
                            tupleCounts: ConstraintCollection[TupleCount],
                            foreignKeys: ConstraintCollection[InclusionDependency])
                           (implicit metadataStore: MetadataStore, planBuilder: PlanBuilder)
  : DataQuanta[(Int, Int, Double)] = {

    val idUtils = metadataStore.getIdUtils

    // q ... total number of join edges
    // get the number of dependent columns
    val q1 = metadataStore.loadConstraints(foreignKeys)
      .map(ind => (ind.getDependentColumnIds.apply(0), 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
    // get the number of referenced columns
    val q2 = metadataStore.loadConstraints(foreignKeys)
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
    val numberTuples = metadataStore.loadConstraints(tupleCounts)
      .map(tp => (tp.getTableId, tp.getNumTuples))

    // logRqEntT ... sum of log(numTuples) and qEntT
    val logRqEntT = qEntT
      .keyBy(_._1).join(numberTuples.keyBy(_._1)).assemble((a, b) => (a._1, a._2 + math.log(b._2)))

    // edgeCol ... Start and target of edge (COLUMN-ID used here)
    val edgeCol = metadataStore.loadConstraints(foreignKeys)
      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
      .flatMap(ind => Seq(ind, ind.swap))

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
    probability
  }

  /**
    * Finding the table importance based on the approach by Yang et al., 2009
    *
    * @param tupleCount       is the constraint collection tuple count
    * @param columnStatistics is constraint collection column statistics
    * @param epsilon          if Euclidean Distance between V and V+1  is
    *                         lower epsilon, the stationary distribution is reached
    * @param maxNumIteration  maximum Number of Iterations
    * @return the solution vector containing the table importance
    */
  def calculate(tupleCount: ConstraintCollection[TupleCount],
                inclusionDependency: ConstraintCollection[InclusionDependency],
                columnStatistics: ConstraintCollection[ColumnStatistics],
                epsilon: Option[Double],
                maxNumIteration: Option[Int])
               (implicit metadataStore: MetadataStore, planBuilder: PlanBuilder)
  : DataQuanta[TableImportance] = {
    // Calculating the probability matrix
    val probabilityMatrix = tableTransitionMatrix(columnStatistics, tupleCount, inclusionDependency)
    // Setting up the initial solution vector
    val Vinitial = initiateVector(tupleCount, metadataStore, columnStatistics)
    (maxNumIteration match {
      case Some(maxNumIteration) => Vinitial.repeat(maxNumIteration, Vold => iteratingImportance(probabilityMatrix, Vold))
      case None => Vinitial.doWhile[Double](_.head < epsilon.getOrElse(1e-10), { Vold =>
        val Vnew = iteratingImportance(probabilityMatrix, Vold)
        val Vdiff = diffV(Vnew, Vold).map(a => math.sqrt(a))
        (Vnew, Vdiff.filter { x => println(x); true })
      })
    }).map { case (tableId, score) => TableImportance(tableId, score) }
  }


  // Calculating the the product of the probability vector and the solution vector
  private def iteratingImportance(probMatrix: DataQuanta[(Int, Int, Double)],
                          V: DataQuanta[(Int, Double)]): DataQuanta[(Int, Double)] = {
    return probMatrix
      .keyBy(a => a._1).join(V.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))
      .reduceByKey((_._2), (a, b) => (a._1, a._2, a._3 + b._3))
      .map(a => (a._2, a._3))
  }

  // Calculating the Euclidean Distance between the solution vector V and V+1
  private def diffV(Vnew: DataQuanta[(Int, Double)],
            Vold: DataQuanta[(Int, Double)]): DataQuanta[(Double)] =
    Vnew.keyBy(_._1).join(Vold.keyBy(_._1)).assemble((a, b) => (a._2, b._2))
      .map(a => ((a._1 - a._2) * (a._1 - a._2))).reduce(_ + _)


  // Initiating the solution vector
  private def initiateVector(tupleCount: ConstraintCollection[TupleCount],
                             metadataStore: MetadataStore,
                             columnStatistics: ConstraintCollection[ColumnStatistics])
                             (implicit planBuilder: PlanBuilder):
  DataQuanta[(Int, Double)] = {
    val idUtils = metadataStore.getIdUtils
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
    Rentropy.map(a => (a._1, a._2));
  }
}

case class TableImportance(tableId: Int, score: Double)