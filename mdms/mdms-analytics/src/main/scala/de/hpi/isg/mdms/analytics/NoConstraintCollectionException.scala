package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.ConstraintCollection

/**
  * Describes that multiple [[ConstraintCollection]]s serve a certain query and, thus, form a conflict.
  */
class NoConstraintCollectionException(message: String) extends RuntimeException(message)
