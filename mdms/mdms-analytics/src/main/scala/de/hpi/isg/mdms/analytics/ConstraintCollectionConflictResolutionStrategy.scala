package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

/**
  * In case that there are multiple [[ConstraintCollection]]s matching some query, this trait describes how to
  * pick one (or more) [[ConstraintCollection]]s to avoid conflicts.
  */
trait ConstraintCollectionConflictResolutionStrategy {

  /**
    * From the conflicting given [[ConstraintCollection]]s, pick the ones that resolve the conflict.
    *
    * @param conflictDomain the conflicting [[ConstraintCollection]]s
    * @tparam T the [[Constraint]] type
    * @return the resolved `conflictDomain`
    * @throws ConstraintCollectionConflictException if the conflict could not be resolved
    */
  @throws(classOf[ConstraintCollectionConflictException])
  def resolve[T](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]]

}
