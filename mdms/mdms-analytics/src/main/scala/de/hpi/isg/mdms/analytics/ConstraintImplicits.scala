package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * @author Lawrence Benson
  *
  * Represents all implicit methods on [[ConstraintCollection]] for analytical purposes.
  */

object ConstraintImplicits {

  implicit class ConstraintCollectionQueryObject[T<: Constraint](constraintCollection: ConstraintCollection[T]) {

    /**
      * Turns the ConstraintCollection into a Scala Iterable[Constraint].
      */
    def constraintsIter: Iterable[Constraint] = {
      constraintCollection.getConstraints.asScala
    }

    /**
      * Returns the size of a ConstraintCollection.
      */
    def count: Int = {
      constraintCollection.getConstraints.size
    }

    /**
      * Returns the subset of the ConstraintCollection in which all Elements are of type A.
      */
    def allOfType[A <: Constraint](constraintClass: Class[A]): Iterable[A] = {
      constraintCollection.constraintsIter.filter(_.getClass == constraintClass).map(_.asInstanceOf[A])
    }

    /**
      * Converts all Constraints in the ConstraintCollection to type A.
      * Throws a cast exception if this is not possible.
      */
    def asType[A <: Constraint]: Iterable[A] = {
      constraintCollection.constraintsIter.map(_.asInstanceOf[A])
    }

    /**
      * Groups all Constraints in a ConstraintCollection according to their Constraint type.
      */
    def groupByType(): Map[Class[_ <: Constraint], Iterable[Constraint]] = {
      constraintCollection.group(_.getClass)
    }

    /**
      * Groups all Constraints in a ConstraintCollection according to the passed grouping function.
      * @param groupFunc Takes a Constraint and returns a key on which to group.
      * @tparam A Type of the key from the groupFunc.
      */
    def group[A](groupFunc: (Constraint) => A): Map[A, Iterable[Constraint]] = {
      constraintCollection.constraintsIter.groupBy(groupFunc)
    }

    /**
      * Creates an intermediate join representation between two ConstraintCollections.
      * @param other ConstraintCollection to join with.
      * @tparam A Type of the left hand side.
      * @tparam B Type of the right hand side.
      */
    def join[A <: Constraint, B <: Constraint](other: ConstraintCollection[B]): UnJoinedConstraintCollection[A, B] = {
      constraintCollection.join[A, B](other.asType[B])
    }

    /**
      * Creates an intermediate join representation between a ConstraintCollection and a Constraint Iterable.
      * @param other Constraint Iterable to join with.
      * @tparam A Type of the left hand side.
      * @tparam B Type of the right hand side.
      */
    def join[A <: Constraint, B <: Constraint](other: Iterable[B]): UnJoinedConstraintCollection[A, B] = {
      constraintCollection.asType[A].join[B](other)
    }

  }


  implicit class ConstraintCollectionIterable(collections: java.util.Collection[ConstraintCollection[_]]) {

    // TODO: Add all constraints with correct description tag
    private val nameMapping = Map("FD" -> "FD", "IND" -> "IND", "UCC" -> "UCC", "CS" -> "column statistics")

    /**
      * Gets the ConstraintCollection by its name from nameMapping.
      */
    def getCollectionByName(name: String): ConstraintCollection[_] = {
      if (!nameMapping.contains(name)) {
        throw new NoSuchElementException(s"No ConstraintCollection with that identifier! Choose from ${nameMapping.keys}")
      }
      val fullName = nameMapping(name)
      val collectionOpt = collections.asScala.find { coll =>
        val description = coll.getDescription
        if (description != null) {
          description.contains(fullName)
        } else {
          false
        }
      }

      collectionOpt.getOrElse(throw new NoSuchElementException(s"No ConstraintCollection found with the id $name"))
    }
  }


  implicit class ConstraintIterable[A <: Constraint](constraints: Iterable[A]) {

    /**
      * Creates an intermediate join representation between two ConstraintCollections.
      * @param other ConstraintCollection to join with.
      * @tparam B Type of the right hand side.
      */
    def join[B <: Constraint](other: ConstraintCollection[B]): UnJoinedConstraintCollection[A, B] = {
      constraints.join[B](other.asType[B])
    }

    /**
      * Creates an intermediate join representation between a ConstraintCollection and a Constraint Iterable.
      * @param other ConstraintCollection to join with.
      * @tparam B Type of the right hand side.
      */
    def join[B <: Constraint](other: Iterable[B]): UnJoinedConstraintCollection[A, B] = {
      new UnJoinedConstraintCollection[A, B](constraints, other)
    }
  }
}
