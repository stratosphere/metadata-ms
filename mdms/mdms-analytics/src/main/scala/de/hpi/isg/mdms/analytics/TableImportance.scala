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
  def apply(idUtils: IdUtils,
            columnStatistics: ConstraintCollection[ColumnStatistics],
            tupleCount: ConstraintCollection[TupleCount],
            inclusionDependency: ConstraintCollection[InclusionDependency],
            metadataStore: MetadataStore
            ) :DataQuanta[(Int, Int, Double)] = {

    implicit val planBuilder = new PlanBuilder(new RheemContext().withPlugin(Java.basicPlugin))

    // numJoinEdges ... total number of join edges
    val numJoinEdges = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (ind.getDependentColumnIds.apply(0),1))
      .reduceByKey(_._1,(a,b)=>(a._1,a._2+b._2))

    // numJoinEdgesEntropy ... product of q and entropy (for each column)
    val numJoinEdgesEntropy = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (cs.getColumnId,cs.getEntropy))
      .keyBy(cs => cs._1).keyJoin(numJoinEdges.keyBy(_._1)).assemble((cs,nJE) => (nJE._1,cs._2*nJE._2))

    // numJoinEdgesEntropyTable ... product of q and entropy summed over the entire table
    val numJoinEdgesEntropyTable = numJoinEdgesEntropy
      .map(cs => (idUtils.getTableId(cs._1),cs._2))
      .reduceByKey(_._2,(a,b)=>(a._1,a._2+b._2))

    // numTuples ... number of tuples of table
    val numTuples = metadataStore.loadConstraints(tupleCount)
      .map(tp => (tp.getTableId,tp.getNumTuples))

    // numJoinEdgesEntropyRTable ... sum of log(numTuples) and numJoinEdgesEntropyTable
    val numJoinEdgesEntropyRTable = numJoinEdgesEntropyTable
        .keyBy(cs => cs._1).keyJoin(numTuples.keyBy(_._1)).assemble((cs,ra) => (ra._1,cs._2 + math.log(ra._2)))

    // ent ... entropy (mapped over table ids!)
    val entropy = metadataStore.loadConstraints(columnStatistics)
      .map(cs => (idUtils.getTableId(cs.getColumnId),cs.getEntropy))

    // probability ... probability of each table
    val probTable = entropy
      .keyBy(cs => cs._1).keyJoin(numJoinEdgesEntropyRTable.keyBy(_._1))
      .assemble((cs,nJEERT) => (cs._1,cs._2/nJEERT._2))

    // start_target ... Start and target of edge
    val edgeStartTarget = metadataStore.loadConstraints(inclusionDependency)
      .map(ind => (idUtils.getTableId(ind.getDependentColumnIds.apply(0)),
        idUtils.getTableId(ind.getReferencedColumnIds.apply(0))))

    // Create Tuple with [Start, Target, Probability]
    val startEdgeProbTable = edgeStartTarget
      .keyBy(cs => cs._1).keyJoin(probTable.keyBy(_._1)).assemble((cs,qa) => (cs._1,cs._2,qa._2))

    // Calculate the probability P[R,S]
    val probabilityRS = startEdgeProbTable.
      filter(cs => cs._1 != cs._2).reduceByKey(t=>(t._1,t._2),(a,b)=>(a._1,a._2,a._3+b._3))

    // Getting dummy values for P[R,R]
    val dummyProbabilityRR = metadataStore.loadTables().map(tableMock=>(tableMock.id,tableMock.id,0.0))

    // Union between P[R,S] + (dummy) P[R,R]
    val dummyProbabilityRSRR = probabilityRS.union(dummyProbabilityRR)

    // Calculate final P[R,R]
    val probabilityRR = dummyProbabilityRSRR.reduceByKey(_._1,(a,b)=>(a._1,a._2,a._3+b._3))
      .map(a=>(a._1,a._1,1-a._3))

    // Union between P[R,S] + P[R,R]
    val probability = probabilityRR.union(probabilityRS)

    return probability
  }
}

