package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval

import scala.collection.JavaConversions._

/**
  * Implementation of the table importance algorithm described in
  * <p>Summarizing relational databases. <i>Yang et al.</i>, Proceedings of the VLDB Endowment 2(1), 2009.</p>
  *
  * @author Marius Walter
  * @author Sebastian Kruse
  */
object TableImportance {

  /**
    * Calculate the information content of database tables.
    *
    * @param columnStatistics is a constraint collection column statistics
    * @param tupleCounts      is a constraint collection tuple count
    * @param metadataStore    is the [[MetadataStore]]
    * @param planBuilder      allows to execute Rheem
    * @return [[DataQuanta]] containing of `(table ID, information content)` tuples
    */
  def calculateInformationContent(columnStatistics: ConstraintCollection[ColumnStatistics],
                                  tupleCounts: ConstraintCollection[TupleCount],
                                  foreignKeys: ConstraintCollection[InclusionDependency] = null)
                                 (implicit metadataStore: MetadataStore,
                                  planBuilder: PlanBuilder)
  : DataQuanta[(Int, Double)] = {

    val idUtils = metadataStore.getIdUtils

    // Calculate the sum of column entropies.
    val columnEntropy = metadataStore.loadConstraints(columnStatistics)
      .map(stat => (idUtils.getTableId(stat.getColumnId), math.max(stat.getEntropy, 1)))
      .reduceByKey(_._1, { case (entropy1, entropy2) => (entropy1._1, entropy1._2 + entropy2._2) })

    // Calculate the key entropy for the table.
    val rowEntropy = metadataStore.loadConstraints(tupleCounts)
      .map(count => (count.getTableId, if (count.getNumTuples > 0) math.log(count.getNumTuples) / math.log(2) else 0))

    // Merge the two entropies.
    val importances = columnEntropy.keyBy(_._1)
      .join(rowEntropy.keyBy(_._1))
      .assemble { case (entropy1, entropy2) => (entropy1._1, math.max(entropy1._2 + entropy2._2, 1)) }

    // If foreign keys are given, supply importances for empty tables.
    if (foreignKeys != null)
      metadataStore.loadConstraints(foreignKeys)
          .flatMap(fk =>  Seq(idUtils.getTableId(fk.getDependentColumnIds()(0)), idUtils.getTableId(fk.getReferencedColumnIds()(0))))
      .distinct
      .map(_ -> 0d)
      .union(importances)
      .reduceByKey(_._1, { case ((table, i1), (_, i2)) => (table, i1 + i2)})
    else importances
  }

  /**
    * Describes an entry in the table transition matrix.
    *
    * @param source      the source table ID
    * @param dest        the destination table ID
    * @param probability the probability of the transition
    */
  case class Transition(source: Int, dest: Int, probability: Double) {
    def +(that: Transition): Transition = {
      require(this.source == that.source && this.dest == that.dest)
      Transition(source, dest, this.probability + that.probability)
    }
  }

  /** Calculating the probability matrix, later used for calculating the table importance.
    *
    * @param columnStatistics is constraint collection column statistics
    * @param tupleCounts      is constraint collection tuple count
    * @param foreignKeys      foreign keys of the schema
    * @param metadataStore    is the [[MetadataStore]]
    * @return the probability matrix as [[DataQuanta]] of `(source table ID, target table ID, transition weight)` tuples
    */
  def calculateTableTransitions(columnStatistics: ConstraintCollection[ColumnStatistics],
                                tupleCounts: ConstraintCollection[TupleCount],
                                foreignKeys: ConstraintCollection[InclusionDependency])
                               (implicit metadataStore: MetadataStore,
                                planBuilder: PlanBuilder)
  : DataQuanta[Transition] = {

    val idUtils = metadataStore.getIdUtils

    // Load the various column entropies.
    val columnEntropies = metadataStore.loadConstraints(columnStatistics)
      .map(stat => (stat.getColumnId, math.max(stat.getEntropy, 1))) // We use a minimum entropy to avoid one-way edges.

    // Find the relevant foreign keys (which are placed between non-empty tables).
    val nonEmptyTableIds = tupleCounts.getConstraints.filterNot(_.getNumTuples == 0).map(_.getTableId).toSet
    val relevantForeignKeys = metadataStore.loadConstraints(foreignKeys)
      .filter(ind => nonEmptyTableIds(idUtils.getTableId(ind.getDependentColumnIds()(0))) &&
        nonEmptyTableIds(idUtils.getTableId(ind.getReferencedColumnIds()(0))))

    // Determine how often each column appears in a (relevant) join relationship.
    val columnFrequencies = relevantForeignKeys
      .flatMap(ind => Seq(ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)),
        selectivity = ProbabilisticDoubleInterval.ofExactly(2)
      )
      .map(column => (column, 1))
      .reduceByKey(_._1, { case ((column, count1), (_, count2)) => (column, count1 + count2) })

    // Determine the "key entropies".
    val keyEntropies = metadataStore.loadConstraints(tupleCounts)
      .map(count => (count.getTableId, if (count.getNumTuples > 0) math.log(count.getNumTuples) / math.log(2) else 0))

    // Determine the information mass of each table.
    val informationMasses = columnEntropies.keyBy(_._1).join(columnFrequencies.keyBy(_._1))
      .assemble { case ((column, entropy), (_, frequency)) => (column, entropy * (frequency + 1)) }
      .map { case (columnId, entropy) => (idUtils.getTableId(columnId), entropy) }
      .union(keyEntropies)
      .reduceByKey(_._1, { case (entropy1, entropy2) => (entropy1._1, entropy1._2 + entropy2._2) })

    // Calculate the non-reflexive transition probabilities.
    val nonReflexiveTransitions = relevantForeignKeys
      .flatMap(ind => Seq((ind.getDependentColumnIds.apply(0), ind.getReferencedColumnIds.apply(0)),
        (ind.getReferencedColumnIds.apply(0), ind.getDependentColumnIds.apply(0))),
        selectivity = ProbabilisticDoubleInterval.ofExactly(2)
      ).withName("Make FKs undirected")

      .map { case (sourceColumn, destColumn) =>
        (idUtils.getTableId(sourceColumn), idUtils.getTableId(destColumn), sourceColumn)
      }.withName("Generalize join edges to table transitions")

      .keyBy(_._3).join(columnEntropies.keyBy(_._1))
      .assemble { case ((sourceTable, destTable, _), (_, entropy)) => (sourceTable, destTable, entropy) }
      .withName("Find entropy for join edge")

      .keyBy(_._1).join(informationMasses.keyBy(_._1))
      .assemble { case ((sourceTable, destTable, entropy), (_, mass)) =>
        Transition(sourceTable, destTable, if (mass > 0) entropy / mass else 0)
      }
      .withName("Calculate join edge probability")

      .reduceByKey(transition => (transition.source, transition.dest), _ + _)
      .withName("Add co-occurring join edges")

    // Calculate the reflexive transition probabilities.
    val reflexiveTransitions = metadataStore.loadConstraints(foreignKeys)
      .flatMap(fk => Seq(idUtils.getTableId(fk.getDependentColumnIds()(0)), idUtils.getTableId(fk.getReferencedColumnIds()(0))))
      .distinct
      .keyBy(id => id).coGroup(nonReflexiveTransitions.keyBy(_.source))
      .assemble { case (tableIds, transitions) =>
        var reflexiveProbability = 1d
        if (transitions != null) {
          for (transition: Transition <- transitions) reflexiveProbability -= transition.probability
        }
        val tableId = tableIds.head
        Transition(tableId, tableId, reflexiveProbability)
      }

    nonReflexiveTransitions.union(reflexiveTransitions)
  }

  /**
    * Finding the table importance based on the approach by Yang et al., 2009
    *
    * @param tupleCounts      is the constraint collection tuple count
    * @param columnStatistics is constraint collection column statistics
    * @param epsilon          if the Chebyshev distance between two subsequent solutions is
    *                         lower than `epsilon`, the stationary distribution is reached
    * @param iterations       number of iterations
    * @return the solution vector containing the table importance
    */
  def calculate(foreignKeys: ConstraintCollection[InclusionDependency],
                tupleCounts: ConstraintCollection[TupleCount],
                columnStatistics: ConstraintCollection[ColumnStatistics],
                epsilon: Double = Double.NaN,
                iterations: Int = 20)
               (implicit metadataStore: MetadataStore,
                planBuilder: PlanBuilder)
  : DataQuanta[TableImportance] = {

    // Calculating the probability matrix
    val transistions = calculateTableTransitions(columnStatistics, tupleCounts, foreignKeys)

    // Setting up the initial solution vector
    val initialTableImportances = calculateInformationContent(columnStatistics, tupleCounts, foreignKeys)
      .map { case (tableId, importance) => TableImportance(tableId, importance) }

    // Do the iterative calculation.
    if (!epsilon.isNaN) {
      initialTableImportances.doWhile[Double](_.head < epsilon, { tableImportances =>
        val newTableImportances = evolveImportances(tableImportances, transistions)

        val delta = newTableImportances.keyBy(_.tableId).join(tableImportances.keyBy(_.tableId))
          .assemble { case (newTableImportance, oldTableImportance) =>
            math.abs(newTableImportance.score - oldTableImportance.score)
          }
          .reduce(math.max)


        (newTableImportances, delta)
      })
    } else {
      initialTableImportances.repeat(iterations, { tableImportances =>
        evolveImportances(tableImportances, transistions)
      })
    }
  }

  /**
    * Perform one iteration step to bring intermediate importances closer to their final value.
    *
    * @param tableImportances the current table importances
    * @param transitions      the transition matrix
    * @return the new table importances
    */
  private def evolveImportances(tableImportances: DataQuanta[TableImportance],
                                transitions: DataQuanta[Transition]): DataQuanta[TableImportance] = {
    transitions.keyBy(_.source).join(tableImportances.keyBy(_.tableId))
      .assemble { case (transition, tableImportance) =>
        TableImportance(transition.dest, tableImportance.score * transition.probability)
      }
      .reduceByKey(_.tableId, _ + _)
  }

}

/**
  * Describes the importance of a table.
  *
  * @param tableId the ID of the described table
  * @param score   the importance score
  */
case class TableImportance(tableId: Int, score: Double) {
  def +(that: TableImportance): TableImportance = {
    require(this.tableId == that.tableId)
    TableImportance(tableId, this.score + that.score)
  }
}