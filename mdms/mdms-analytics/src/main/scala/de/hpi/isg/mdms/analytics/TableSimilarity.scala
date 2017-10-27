package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable

object TableSimilarity {

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

    val distinctValuesMap = columnStatistics.getConstraints
      .map(stat => (stat.getColumnId, stat.getNumDistinctValues))
      .toMap

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
      (math.min(depTable, refTable), math.max(depTable, refTable)) ->
        distinctValuesA.toDouble * distinctValuesA / tupleCountsA / tupleCountsB
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


  //  def getFanout(tupleCount: ConstraintCollection[TupleCount],
  //                metadataStore: MetadataStore
  //               ): DataQuanta[(Int, Double)] = {
  //    val numberTuples = metadataStore.loadConstraints(tupleCount)
  //      .map(tp => (tp.getTableId, tp.getNumTuples.toDouble))
  //
  //    return numberTuples
  //  }
  //
  //  def getStrength(idUtils: IdUtils,
  //                  columnStatistics: ConstraintCollection[ColumnStatistics],
  //                  tupleCount: ConstraintCollection[TupleCount],
  //                  inclusionDependency: ConstraintCollection[InclusionDependency],
  //                  metadataStore: MetadataStore,
  //                  matchingFraction: DataQuanta[(Int, Double)],
  //                  fanout: DataQuanta[(Int, Double)]
  //                 ): DataQuanta[(Int, Int, Double)] = {
  //
  //    val strength1 = metadataStore.loadConstraints(inclusionDependency)
  //      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
  //      .flatMap(ind => Seq(ind, ind.swap))
  //      .keyBy(_._1).join(matchingFraction.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
  //      .keyBy(_._2).join(matchingFraction.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))
  //
  //    val strength2 = metadataStore.loadConstraints(inclusionDependency)
  //      .map(ind => (ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)))
  //      .flatMap(ind => Seq(ind, ind.swap))
  //      .keyBy(_._1).join(fanout.keyBy(_._1)).assemble((a, b) => (a._1, a._2, b._2))
  //      .keyBy(_._2).join(fanout.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 * b._2))
  //
  //    val strength = strength1
  //      .keyBy(_._1).join(strength2.keyBy(_._1)).assemble((a, b) => (a._1, a._2, a._3 / b._3))
  //      .map(x => (idUtils.getTableId(x._1), idUtils.getTableId(x._2), x._3))
  //      .reduce((a, b) => if (a._2 > b._2) a else b)
  //
  //    val dummyStrength = metadataStore.loadTables().map(tableMock => (tableMock.id, tableMock.id, 1.0))
  //
  //    return strength.union(dummyStrength)
  //  }
  //
  //  def distance(strength: DataQuanta[(Int, Int, Double)])
  //  : DataQuanta[(Int, Int, Double)] = {
  //
  //    return strength.map(x => (x._1, x._2, 1.0 - x._3))
  //  }


}

case class TableSimilarity(tableId1: Int, tableId2: Int, similarity: Double)

