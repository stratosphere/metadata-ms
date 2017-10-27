package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}


class TableClustering {

//  def clustering(numberClusters: Int,
//                 tableImportance: DataQuanta[(Int, Double)],
//                 cs: ConstraintCollection[ColumnStatistics],
//                 tp: ConstraintCollection[TupleCount],
//                 id: ConstraintCollection[InclusionDependency],
//                 store: MetadataStore,
//                 matchingFraction: DataQuanta[(Int, Double)],
//                 fanout: DataQuanta[(Int, Double)])
//                (implicit planBuilder: PlanBuilder)
//  : DataQuanta[(Int, Int)] = {
//
//    val tableSimilarity = new TableSimilarity
//
//    val iniCluster = getInitialCluster(tableImportance) // setup initial clusters
//    val similarity = tableSimilarity.getStrength(store.getIdUtils, cs, tp, id, store, matchingFraction, fanout)
//
//    sys.error("Not yet implemented.")
//  }
//
//  def getInitialCluster(tableImportance: DataQuanta[(Int, Double)]): DataQuanta[(Int, Int)] = {
//    val initialCluster = tableImportance.reduce((a, b) => if (a._2 > b._2) a else b)
//      .map(x => (0, x._1))
//
//    return initialCluster
//  }
}

//work on the selection of the centroid selection
/*
class SelectNearestCentroid {

 /** Keeps the broadcasted center. */
 var center: util.Collection[TaggedPoint] = _

 override def open(executionCtx: ExecutionContext) = {
   centroids = executionCtx.getBroadcast[TaggedPoint]("centroids")
 }

 override def apply(point: Point): TaggedPointCounter = {
   var minDistance = Double.PositiveInfinity
   var nearestCentroidId = -1
   for (centroid <- centroids) {
     val distance = point.distanceTo(centroid)
     if (distance < minDistance) {
       minDistance = distance
       nearestCentroidId = centroid.centroidId
     }
   }
   new TaggedPointCounter(point, nearestCentroidId, 1)
 }
}
*/
