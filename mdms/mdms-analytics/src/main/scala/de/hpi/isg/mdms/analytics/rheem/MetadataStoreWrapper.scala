package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, FunctionalDependency, InclusionDependency, UniqueColumnCombination}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.Constraint
import org.qcri.rheem.api._

import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Exposes metadata from a [[de.hpi.isg.mdms.model.MetadataStore]] as [[org.qcri.rheem.api.DataQuanta]].
  */
class MetadataStoreWrapper(metadataStore: MetadataStore) {

  def loadINDs(constraintCollectionId: Int)(implicit planBuilder: PlanBuilder) =
    loadConstraints[InclusionDependency](constraintCollectionId)

  def loadFDs(constraintCollectionId: Int)(implicit planBuilder: PlanBuilder) =
    loadConstraints[FunctionalDependency](constraintCollectionId)

  def loadUCCs(constraintCollectionId: Int)(implicit planBuilder: PlanBuilder) =
    loadConstraints[UniqueColumnCombination](constraintCollectionId)

  def loadStatistics(constraintCollectionId: Int)(implicit planBuilder: PlanBuilder) =
    loadConstraints[ColumnStatistics](constraintCollectionId)

  /**
    * Fallback option to load constraints
    * @param constraintCollectionId
    * @param planBuilder
    * @tparam Type
    * @return
    */
  def loadConstraints[Type <: Constraint : ClassTag](constraintCollectionId: Int)(implicit planBuilder: PlanBuilder): DataQuanta[Type] = {
    val constraintCollection = metadataStore.getConstraintCollection(constraintCollectionId)
    require(constraintCollection != null, message = s"No constraint collection with ID $constraintCollectionId found.")
    val constraintCls = classTag[Type].runtimeClass
    val typeConstraints = constraintCollection.getConstraints
      .filter(c => constraintCls.isAssignableFrom(c.getClass))
      .asInstanceOf[Iterable[Type]]
    planBuilder.loadCollection(typeConstraints)
  }

}
