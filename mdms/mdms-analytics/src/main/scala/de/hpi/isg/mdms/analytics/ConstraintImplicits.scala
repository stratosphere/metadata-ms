package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.collection.JavaConverters._

object ConstraintImplicits {

  implicit class ConstraintCollectionQueryObject(constraintCollection: ConstraintCollection) {

    def constraintsIter: Iterable[Constraint] = {
      constraintCollection.getConstraints.asScala
    }

    def count: Int = {
      constraintCollection.getConstraints.size
    }

    def allOfType[T <: Constraint](constraintClass: Class[T]): Iterable[Constraint] = {
      constraintCollection.constraintsIter.filter(_.getClass == constraintClass)
    }

    def groupByType: Map[Class[_ <: Constraint], Iterable[Constraint]] = {
      constraintCollection.group(_.getClass)
    }

    def group[K](groupFunc: Constraint => K): Map[K, Iterable[Constraint]] = {
      constraintCollection.constraintsIter.groupBy(groupFunc)
    }

  }
}
