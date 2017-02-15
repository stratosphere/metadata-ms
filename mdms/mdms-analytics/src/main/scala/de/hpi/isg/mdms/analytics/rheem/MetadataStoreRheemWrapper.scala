package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.targets.Target
import org.qcri.rheem.api._

import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Exposes metadata from a [[de.hpi.isg.mdms.model.MetadataStore]] as [[org.qcri.rheem.api.DataQuanta]].
  */
class MetadataStoreRheemWrapper(metadataStore: MetadataStore) {

  /**
    * Retrieves all [[ConstraintCollection]]s and resolves potential conflicts if multiple ones are found. Those
    * are then delivered as [[DataQuanta]].
    *
    * @param scope                      of the [[ConstraintCollection]]s
    * @param conflictResolutionStrategy decides how to act in the presence of multiple matching [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return [[DataQuanta]] of the [[Constraint]]s in the matching, resolved [[ConstraintCollection]]s
    */
  def loadConstraints[Type <: Constraint : ClassTag]
  (scope: Target,
   conflictResolutionStrategy: ConstraintCollectionConflictResolutionStrategy = FailOnConflictStrategy)
  (implicit planBuilder: PlanBuilder):
  DataQuanta[Type] = {
    val constraintCollections = findConstraintCollections[Type](scope, conflictResolutionStrategy)
    if (constraintCollections.isEmpty)
      throw new NoConstraintCollectionException("Could not find the requested constraint collection.")

    loadConstraints(constraintCollections: _*)
  }

  /**
    * Retrieves all [[ConstraintCollection]]s and resolves potential conflicts if multiple ones are found.
    *
    * @param scope                      of the [[ConstraintCollection]]s
    * @param conflictResolutionStrategy decides how to act in the presence of multiple matching [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return the matching, resolved [[ConstraintCollection]]s
    */
  def findConstraintCollections[Type <: Constraint : ClassTag]
  (scope: Target, conflictResolutionStrategy: ConstraintCollectionConflictResolutionStrategy = FailOnConflictStrategy):
  Seq[ConstraintCollection[Type]] = {
    val typeClass = classTag[Type].runtimeClass.asInstanceOf[java.lang.Class[Type]]
    val constraintCollections = metadataStore.getConstraintCollectionByConstraintTypeAndScope(typeClass, scope).toSeq
    conflictResolutionStrategy.resolve[Type](constraintCollections)
  }

  /**
    * Retrieves all [[ConstraintCollection]]s for the given [[Constraint]] type whose scope is included in the given scope.
    *
    * @param scope of the [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return the matching, resolved [[ConstraintCollection]]s
    */
  def listConstraintCollections[Type <: Constraint : ClassTag]
  (scope: Target):
  Seq[ConstraintCollection[Type]] = {
    val typeClass = classTag[Type].runtimeClass.asInstanceOf[java.lang.Class[Type]]
    metadataStore.getIncludedConstraintCollections(typeClass, scope).toSeq
  }


  /**
    * Provide the [[Constraint]]s of a [[ConstraintCollection]].
    *
    * @param constraintCollectionId the ID of the [[ConstraintCollection]]
    * @param planBuilder            required to set up the Rheem plan
    * @tparam Type [[Constraint]] type
    * @return the [[Constraint]]s of the [[ConstraintCollection]] as [[DataQuanta]]
    */
  def loadConstraints[Type <: Constraint : ClassTag](constraintCollectionId: Int)(implicit planBuilder: PlanBuilder): DataQuanta[Type] = {
    val constraintCollection = metadataStore.getConstraintCollection(constraintCollectionId).asInstanceOf[ConstraintCollection[Type]]
    require(
      constraintCollection != null,
      message = s"No constraint collection with ID $constraintCollectionId found."
    )
    val constraintCls = classTag[Type].runtimeClass
    require(
      constraintCollection.getConstraintClass == constraintCls,
      message = s"Constraint collection is of type ${constraintCollection.getConstraintClass}. Expected $constraintCls, though."
    )

    // TODO: Depending on the underlying storage, there might be more efficient ways to ship the constraints to Rheem.
    planBuilder.loadCollection(constraintCollection.getConstraints)
  }

  def loadConstraints[Type <: Constraint : ClassTag](constraintCollections: ConstraintCollection[Type]*)(implicit planBuilder: PlanBuilder):
  DataQuanta[Type] =
    constraintCollections.size match {
      case 0 => throw new NoConstraintCollectionException("No constraints given.")
      case 1 =>
        val constraintCollection = constraintCollections.head
        val constraintCls = classTag[Type].runtimeClass
        require(
          constraintCollection.getConstraintClass == constraintCls,
          message = s"Constraint collection is of type ${constraintCollection.getConstraintClass}. Expected $constraintCls, though."
        )
        // TODO: Depending on the underlying storage, there might be more efficient ways to ship the constraints to Rheem.
        planBuilder.loadCollection(constraintCollection.getConstraints)
      case _ => constraintCollections
        .map(cc => this.loadConstraints[Type](cc))
        .reduce(_ union _)
    }
}
