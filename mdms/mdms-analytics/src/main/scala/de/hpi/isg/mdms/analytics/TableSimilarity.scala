package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.targets.Table
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * This object provides methods to determine the similarity among tables as described in
  * <p>Yang, X., Procopiuc, C.M. and Srivastava, D., 2009. Summarizing relational databases. Proceedings of the VLDB Endowment, 2(1), pp.634-645.</p>
  *
  */
object TableSimilarity {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Calculate table similarities.
    *
    * @param foreignKeys      foreign keys among the tables
    * @param columnStatistics statistics for the involved tables
    * @param tupleCounts      tuple counts for the involved tables
    * @param transitiveHull   whether to calculate the similarities not only for adjacent tables
    * @param store            that contains all the metadata
    * @param planBuilder      to execute Rheem
    * @return the calculated table similarities as [[DataQuanta]]; only such table pairs where the left table ID is smaller are listed
    */
  def calculate(foreignKeys: ConstraintCollection[InclusionDependency],
                columnStatistics: ConstraintCollection[ColumnStatistics],
                tupleCounts: ConstraintCollection[TupleCount],
                transitiveHull: Boolean = false)
               (implicit store: MetadataStore,
                planBuilder: PlanBuilder):
  DataQuanta[TableSimilarity] = {
    val linkStrengths = calculateStrengths(foreignKeys, columnStatistics, tupleCounts)
      .map { case ((table1, table2), strength) => TableSimilarity(table1, table2, strength) }
    if (transitiveHull) {
      val expandedStrengths = expandStrengths(linkStrengths)
      planBuilder.loadCollection(expandedStrengths)
        .map { case ((table1, table2), strength) => TableSimilarity(table1, table2, strength) }
    } else {
      planBuilder.loadCollection(linkStrengths)
    }
  }

  /**
    * Calculate the strengths between tables that are connected via a foreign key relationship. In case of multiple
    * such edges, the maximum strength is considered.
    *
    * @param foreignKeys      the foreign keys
    * @param columnStatistics statistics about the foreign keys and their referenced columns
    * @param tupleCounts      tuple counts for the foreign keys and their referenced columns
    * @param store            the [[MetadataStore]] in which all the metadata resides
    * @return a [[Map]] that associates `(table ID 1, table ID 2)` (where `table ID 1 < table ID 2`) to their strength
    */
  private def calculateStrengths(foreignKeys: ConstraintCollection[InclusionDependency],
                                 columnStatistics: ConstraintCollection[ColumnStatistics],
                                 tupleCounts: ConstraintCollection[TupleCount])
                                (implicit store: MetadataStore):
  Map[(Int, Int), Double] = {
    val tupleCountMap = tupleCounts.getConstraints
      .map(tc => (tc.getTableId, tc.getNumTuples))
      .toMap
      .withDefaultValue(0)

    val distinctValuesMap = columnStatistics.getConstraints
      .map(stat => (stat.getColumnId, stat.getNumDistinctValues))
      .toMap
      .withDefaultValue(0L)

    val idUtils = store.getIdUtils
    foreignKeys.getConstraints.map { ind =>
      // strength(A, B) =  matching fraction(A)  *  matching fraction(B)
      //                  -----------------------------------------------
      //                  matched avg. fanout(A) * matched avg. fanout(B)
      // matching fraction(A) = (|A| - nulls(A)) / |A|, because A is an FK
      // matched avg. fanout(A) = 1, because B is a PK (every value is unique)
      // matching fraction(B) = distinct values(A) / |B|, because A is a subset of B and B is unique
      // matched avg. fanout(B) (|A| - nulls(A)) / distinct values(A), because A is a subset of B and B is unique
      // strength(A, B) = ((|A| - nulls(A)) / |A|) * (distinct values(A) / |B|)
      //                  ---------------------------------------------
      //                  (|A| - nulls(A)) / distinct values(A)
      //                = (distinct values(A))^2
      //                  ---------------------
      //                        |A| * |B|
      val dep = ind.getDependentColumnIds.apply(0)
      val ref = ind.getReferencedColumnIds.apply(0)
      val distinctValuesA = distinctValuesMap(dep)
      val depTable = idUtils.getTableId(dep)
      val refTable = idUtils.getTableId(ref)
      val tupleCountsA = tupleCountMap(depTable)
      val tupleCountsB = tupleCountMap(refTable)
      (
        (math.min(depTable, refTable), math.max(depTable, refTable)),
        if (tupleCountsA == 0 || tupleCountsB == 0) 0d
        else distinctValuesA.toDouble * distinctValuesA / tupleCountsA / tupleCountsB
      ) match {
        case (link, strength: Double) if strength > 1d =>
          val pp = new DependencyPrettyPrinter(store)
          logger.warn(s"Got strength $strength for ${pp.prettyPrint(ind)}: DV(A)=$distinctValuesA, |A|=$tupleCountsA, |B|=$tupleCountsB. Inverting...")
          (link, 1 / strength)
        case other => other
      }
    }
      .filter { case ((src, dest), _) => src != dest }
      .groupBy(_._1)
      .mapValues(_.map(_._2).fold[Double](0)(math.max))
  }

  /**
    * Calculate the strengths between any connected vertices as the maximum link strength of their path.
    * In case of multiple paths, consider the one with highest overall strength.
    *
    * @param linkStrengths the link (= atomic path) strengths as a [[Map]] that associates `(table ID 1, table ID 2)`
    *                      (where `table ID 1 < table ID 2`) pairs with their strength
    * @return all expanded strengths (including the atomic ones) as a [[Map]]that associates `(table ID 1, table ID 2)`
    *         (where `table ID 1 < table ID 2`) pairs with their strength
    */
  private def expandStrengths(linkStrengths: Iterable[TableSimilarity]):
  scala.collection.Map[(Int, Int), Double] = {
    // We solve this problem using Dijkstra's algorithm (Warshall-Floyd could be an alternative, but we probably have rather sparse graphs).
    val indexedEdges = linkStrengths.flatMap { case TableSimilarity(vertex1, vertex2, strength) =>
      Seq((vertex1, (vertex2, strength)), (vertex2, (vertex1, strength)))
    }
      .groupBy { case (source, _) => source }
      .mapValues(_.map(_._2).toSeq)
    val vertices = linkStrengths.flatMap(ls => Seq(ls.tableId1, ls.tableId2)).toSet
    val expandedStrengths = scala.collection.mutable.Map[(Int, Int), Double]().withDefaultValue(0)
    vertices.foreach(source => updateViaDijsktra(source, indexedEdges, expandedStrengths))
    expandedStrengths
  }

  def updateViaDijsktra(source: Int,
                        indexedEdges: Map[Int, Seq[(Int, Double)]],
                        expandedStrengths: mutable.Map[(Int, Int), Double]):
  Unit = {
    // Prepare.
    val queue = mutable.PriorityQueue[(Int, Double)]((source, 1))(Ordering.by { case (_, strength) => 1 - strength })
    val bestStrengths = mutable.Map[Int, Double]((source, 1)).withDefaultValue(0)
    val visitedNodes = mutable.Set[Int]()

    // Continuously follow the currently strongest vertex.
    while (queue.nonEmpty) {
      val (vertex, pathStrength) = queue.dequeue()
      // Check whether we have not already settled this node.
      if (visitedNodes add vertex) {
        // Add the strength because at this point it cannot get smaller anymore.
        if (vertex != source) {
          expandedStrengths((math.min(source, vertex), math.max(source, vertex))) = pathStrength
        }
        // Follow all edges.
        indexedEdges.getOrElse(vertex, Seq()).foreach { case (nextVertex, linkStrength) =>
          val bestPathStrength = bestStrengths(nextVertex)
          val newPathStrength = pathStrength * linkStrength // Indeed, we use "*" rather than "+".
          if (newPathStrength > bestPathStrength) {
            bestStrengths(nextVertex) = newPathStrength
            queue += Tuple2(nextVertex, newPathStrength)
          }
        }
      }
    }
  }

  /**
    * Build a join tree starting from the given `root` [[Table]].
    *
    * @param root              from which to span the join tree
    * @param foreignKeys       from which to select the tree edges
    * @param tableSimilarities which describe the weight of the tree edges
    * @param store             in which all the metadata reside
    * @param planBuilder       to produce [[DataQuanta]]
    * @return the tree edges as [[DataQuanta]] of [[JoinTreeEdge]]s
    */
  def buildJoinTree(root: Table,
                    foreignKeys: ConstraintCollection[InclusionDependency],
                    tableSimilarities: ConstraintCollection[TableSimilarity])
                   (implicit store: MetadataStore,
                    planBuilder: PlanBuilder):
  DataQuanta[JoinTreeEdge] = {
    // Index the table similarities.
    val tableSimilarityIndex = tableSimilarities.getConstraints
      .map(sim => (sim.tableId1, sim.tableId2) -> sim)
      .toMap

    // Index the foreign keys and collect their weights.
    val idUtils = store.getIdUtils
    val primaryAdjacencyIndex = mutable.Map[Int, mutable.Map[Int, JoinTreeEdge]]()
    foreignKeys.getConstraints.foreach { fk =>
      // Get involved tables.
      val refTableId = idUtils.getTableId(fk.getDependentColumnIds()(0))
      val depTableId = idUtils.getTableId(fk.getReferencedColumnIds()(0))
      val similarity = tableSimilarityIndex
        .get(if (depTableId < refTableId) (depTableId, refTableId) else (refTableId, depTableId)) match {
        case Some(sim) => sim.similarity
        case None => 0
      }

      if (similarity > 0) {
        // Update the edges.
        locally {
          val secondaryAdjacencyIndex = primaryAdjacencyIndex.get(depTableId) match {
            case Some(map) => map
            case None =>
              val map = mutable.Map[Int, JoinTreeEdge]()
              primaryAdjacencyIndex(depTableId) = map
              map
          }
          secondaryAdjacencyIndex.get(refTableId) match {
            case Some(joinTreeEdge) =>
              if (joinTreeEdge.targetSimilarity < similarity)
                secondaryAdjacencyIndex(refTableId) = JoinTreeEdge(depTableId, refTableId, similarity, true)
            case None =>
              secondaryAdjacencyIndex(refTableId) = JoinTreeEdge(depTableId, refTableId, similarity, true)
          }
        }
        locally {
          val secondaryAdjacencyIndex = primaryAdjacencyIndex.get(refTableId) match {
            case Some(map) => map
            case None =>
              val map = mutable.Map[Int, JoinTreeEdge]()
              primaryAdjacencyIndex(refTableId) = map
              map
          }
          secondaryAdjacencyIndex.get(depTableId) match {
            case Some(joinTreeEdge) =>
              if (joinTreeEdge.targetSimilarity < similarity)
                secondaryAdjacencyIndex(depTableId) = JoinTreeEdge(refTableId, depTableId, similarity, false)
            case None =>
              secondaryAdjacencyIndex(depTableId) = JoinTreeEdge(refTableId, depTableId, similarity, false)
          }
        }
      }
    }

    // Execute Dijkstra's algorithm to create the join tree.
    val joinTreeEdges = mutable.Map[Int, JoinTreeEdge]()
    val visitedTables = mutable.Set[Int]()
    val visitingQueue = mutable.PriorityQueue[(Int, Double)]()(Ordering.by(-_._2))
    visitingQueue.enqueue((root.getId, 1d))

    while (visitingQueue.nonEmpty) {
      val (nextTableId, similarity) = visitingQueue.dequeue()
      if (visitedTables.add(nextTableId)) {
        primaryAdjacencyIndex.get(nextTableId).foreach { secondaryAdjacencyIndex =>
          secondaryAdjacencyIndex.valuesIterator.foreach { jte =>
            if (!visitedTables.contains(jte.childTableId)) {
              val candidate = JoinTreeEdge(jte.parentTableId, jte.childTableId, similarity * jte.targetSimilarity, jte.childReferenced)
              if (joinTreeEdges.get(jte.childTableId).forall(_.targetSimilarity < candidate.targetSimilarity)) {
                joinTreeEdges(jte.childTableId) = candidate
                visitingQueue.enqueue((candidate.childTableId, candidate.targetSimilarity))
              }
            }
          }
        }
      }
    }

    planBuilder.loadCollection(joinTreeEdges.values)
  }

}

/**
  * Describes the similarity between two tables.
  *
  * @param tableId1   the ID of the one table
  * @param tableId2   the ID of the other table
  * @param similarity the similarity score
  */
case class TableSimilarity(tableId1: Int, tableId2: Int, similarity: Double)

/**
  * Defines an edge of a join tree.
  *
  * @param parentTableId    the ID of the [[Table]] that forms the parent node in the join tree
  * @param childTableId     the ID of the [[Table]] that forms the child node in the join tree
  * @param targetSimilarity the similarity of the child [[Table]] to the root [[Table]]
  * @param childReferenced  whether the parent [[Table]] references the child [[Table]]
  */
case class JoinTreeEdge(parentTableId: Int, childTableId: Int, targetSimilarity: Double, childReferenced: Boolean)

