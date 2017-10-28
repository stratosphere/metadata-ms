package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * This object provides a clustering as described in
  * <p>Yang, X., Procopiuc, C.M. and Srivastava, D., 2009. Summarizing relational databases. Proceedings of the VLDB Endowment, 2(1), pp.634-645.</p>
  */
object TableClustering {

  /**
    * Apply the k-weighted-center clustering algorithm.
    *
    * @param similarities (partial) similarities between tables; distances are calculated as `1 - similarity`
    * @param importances  the importances of the tables
    * @param k            the number of clusters to create
    * @param store        within which all metadata resides
    * @param planBuilder  used to execute Rheem
    * @return [[DataQuanta]] of [[TableClusterAssignment]]s
    */
  def kWeightedCenter(similarities: ConstraintCollection[TableSimilarity],
                      importances: ConstraintCollection[TableImportance],
                      k: Int)
                     (implicit store: MetadataStore,
                      planBuilder: PlanBuilder):
  DataQuanta[TableClusterAssignment] = {
    // Index the distances among the tables.
    val distanceMap = similarities.getConstraints
      .map { case TableSimilarity(tableId1, tableId2, similarity) => ((tableId1, tableId2), 1 - similarity) }
      .toMap
      .withDefaultValue(1d)

    def distance(table1: Int, table2: Int) =
      if (table1 == table2) 0d
      else distanceMap((math.min(table1, table2), math.max(table1, table2)))

    // Index the table importances.
    val importanceMap = importances.getConstraints
      .map { case TableImportance(tableId, score) => (tableId, score) }
      .toMap

    // Prepare the clustering data structures.
    val clusterCenters = mutable.ArrayBuffer[Int]()
    val clusterAssignments = mutable.Map[Int, Int]()

    // Create the first cluster.
    def keepBest(entry1: (Int, Double), entry2: (Int, Double)) = if (entry1._2 >= entry2._2) entry1 else entry2

    val (firstClusterCenter, _) = importanceMap.reduce(keepBest)
    clusterCenters += firstClusterCenter
    importanceMap.keysIterator.foreach { tableId => clusterAssignments(tableId) = 0 }

    // Repeatedly remove new cluster centers and re-assign the clusters.
    for (newCluster <- 1 until k) {
      // Determine a new center.
      val (newCenter, _) = clusterAssignments
        .map { case (table, cluster) =>
          val center = clusterCenters(cluster)
          val score = importanceMap(table) * distance(table, center)
          (table, score)
        }
        .reduce(keepBest)
      clusterCenters += newCenter

      // Re-assign the nodes.
      importanceMap.keysIterator.foreach { table =>
        if (table == newCenter || {
          val currentCluster = clusterAssignments(table)
          val currentDist = distance(table, clusterCenters(currentCluster))
          val newDist = distance(table, newCenter)
          newDist < currentDist
        }) clusterAssignments(table) = newCluster
      }
    }

    // Produce the final output.
    planBuilder.loadCollection[TableClusterAssignment](for ((table, cluster) <- clusterAssignments)
      yield TableClusterAssignment(table, cluster, clusterCenters(cluster))
    )

  }

}

/**
  * Describes the clustering of a single table.
  *
  * @param tableId       the ID of the clustered table
  * @param clusterId     the ID of the cluster it is in
  * @param centerTableId the ID of the cluster's center table
  */
case class TableClusterAssignment(tableId: Int, clusterId: Int, centerTableId: Int)

