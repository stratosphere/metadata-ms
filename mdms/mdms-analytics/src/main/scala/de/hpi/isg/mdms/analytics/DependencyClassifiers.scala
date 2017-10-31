package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints._
import de.hpi.isg.mdms.java.apps.PrimaryKeyClassifier
import de.hpi.isg.mdms.java.fk.SimpleForeignKeyDetector
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

import scala.collection.JavaConversions._

/**
  * This object provides a Scala-friendly facade to Metacrate's dependency classifiers.
  */
object DependencyClassifiers {

  def classifyPrimaryKeys(uccs: ConstraintCollection[UniqueColumnCombination],
                          statistics: ConstraintCollection[ColumnStatistics],
                          textStatistics: ConstraintCollection[TextColumnStatistics])
                         (implicit store: MetadataStore, planBuilder: PlanBuilder):
  DataQuanta[UniqueColumnCombination] = planBuilder.loadCollection(
    PrimaryKeyClassifier.classifyPrimaryKeys(store, uccs, statistics, textStatistics)
  )


  def classifyForeignKeys(inds: ConstraintCollection[InclusionDependency],
                          uccs: Iterable[ConstraintCollection[UniqueColumnCombination]],
                          tupleCounts: Iterable[ConstraintCollection[TupleCount]],
                          statistics: Iterable[ConstraintCollection[ColumnStatistics]],
                          textStatistics: Iterable[ConstraintCollection[TextColumnStatistics]],
                          neglectEmptyTables: Boolean = false)
                         (implicit store: MetadataStore, planBuilder: PlanBuilder):
  DataQuanta[InclusionDependency] = {
    val fks = SimpleForeignKeyDetector.detect(
      inds, uccs, tupleCounts, statistics, textStatistics, neglectEmptyTables
    )
    planBuilder.loadCollection(fks).map(_.ind)
  }

}
