package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * @author Lawrence Benson
  */


object ConstraintImplicits {

  implicit class ConstraintCollectionQueryObject(constraintCollection: ConstraintCollection) {

    def constraintsIter: Iterable[Constraint] = {
      constraintCollection.getConstraints.asScala
    }

    def count: Int = {
      constraintCollection.getConstraints.size
    }

    def allOfType[A <: Constraint](constraintClass: Class[A]): Iterable[A] = {
      constraintCollection.constraintsIter.filter(_.getClass == constraintClass).map(_.asInstanceOf[A])
    }

    def asType[A <: Constraint]: Iterable[A] = {
      constraintCollection.constraintsIter.map(_.asInstanceOf[A])
    }

    def groupByType: Map[Class[_ <: Constraint], Iterable[Constraint]] = {
      constraintCollection.group(_.getClass)
    }

    def group[A](groupFunc: (Constraint) => A): Map[A, Iterable[Constraint]] = {
      constraintCollection.constraintsIter.groupBy(groupFunc)
    }

    def join[A <: Constraint, B <: Constraint](other: ConstraintCollection): UnJoinedConstraintCollection[A, B] = {
      constraintCollection.join[A, B](other.asType[B])
    }

    def join[A <: Constraint, B <: Constraint](other: Iterable[B]): UnJoinedConstraintCollection[A, B] = {
      constraintCollection.asType[A].join[A, B](other)
    }

  }

  implicit class ConstraintIterable(constraints: Iterable[_ <: Constraint]) {

    def join[A <: Constraint, B <: Constraint](other: ConstraintCollection): UnJoinedConstraintCollection[A, B] = {
      constraints.join[A, B](other.asType[B])
    }

    def join[A <: Constraint, B <: Constraint](other: Iterable[B]): UnJoinedConstraintCollection[A, B] = {
      new UnJoinedConstraintCollection[A, B](constraints.map(_.asInstanceOf[A]), other)
    }
  }
}
