package de.hpi.isg.mdms

import de.hpi.isg.mdms.analytics.rheem.MetadataStoreRheemWrapper
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.language.implicitConversions

/**
  * Provides utilities to analyze metadata store contents.
  */
package object analytics {

  /**
    * Expose Rheem capabilities on a [[MetadataStore]].
    *
    * @param metadataStore the [[MetadataStore]]
    * @return a [[MetadataStoreRheemWrapper]] exposing Rheem capabilities
    */
  implicit def wrapForRheem(metadataStore: MetadataStore): MetadataStoreRheemWrapper =
    new MetadataStoreRheemWrapper(metadataStore)

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always fails on conflicts.
    */
  object FailOnConflictStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T <: Constraint](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) throw new ConstraintCollectionConflictException(conflictDomain)
      conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always takes the first element from the conflict domain.
    */
  object TakeFirstStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T <: Constraint](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) Seq(conflictDomain.head)
      else conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always takes the first element from the conflict domain.
    */
  object TakeLastStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T <: Constraint](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) Seq(conflictDomain.last)
      else conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that resolves conflict by taking all [[ConstraintCollection]]s.
    */
  object TakeAllStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T <: Constraint](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] =
      conflictDomain
  }

}
