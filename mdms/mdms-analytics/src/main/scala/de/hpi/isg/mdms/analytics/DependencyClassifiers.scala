package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TextColumnStatistics, UniqueColumnCombination}
import de.hpi.isg.mdms.java.apps.PrimaryKeyClassifier
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

/**
  * This object provides a Scala-friendly facade to Metacrate's dependency classifiers.
  */
object DependencyClassifiers {

  def classifyPrimaryKeys(uccs: ConstraintCollection[UniqueColumnCombination],
                          statistics: ConstraintCollection[ColumnStatistics],
                          textStatistics: ConstraintCollection[TextColumnStatistics],
                          resultId: String = null)
                         (implicit store: MetadataStore, planBuilder: PlanBuilder):
  ConstraintCollection[UniqueColumnCombination] = {
    val resultCCId = PrimaryKeyClassifier.fromParameters(
      store, uccs.getId, statistics.getId, textStatistics.getId, resultId
    )
    store.getConstraintCollection[UniqueColumnCombination](resultCCId)
  }

  def classifyForeignKeys(inds: ConstraintCollection[InclusionDependency],
                          statistics: ConstraintCollection[ColumnStatistics],
                          textStatistics: ConstraintCollection[TextColumnStatistics])
                         (implicit store: MetadataStore, planBuilder: PlanBuilder):
  DataQuanta[InclusionDependency] = {
    null
  }

}
