package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.ConstraintCollection

/**
  * Describes that multiple [[ConstraintCollection]]s serve a certain query and, thus, form a conflict.
  */
class ConstraintCollectionConflictException(message: String) extends RuntimeException(message) {

  def this(constraintCollections: Iterable[ConstraintCollection[_]]) =
    this(s"The constraint collections with the IDs ${constraintCollections.mkString(", ")} are conflicting.")

}
