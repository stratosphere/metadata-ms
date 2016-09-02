package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.Constraint

/**
  * @author Lawrence Benson
  *
  * Contains all intermediate and final objects required for JOINs on [[de.hpi.isg.mdms.model.constraints.ConstraintCollection]].
  *
  * Step by step JOIN representation:
  * ConstraintCollections are converted to [[Iterable]][([[de.hpi.isg.mdms.model.constraints.Constraint]], [[de.hpi.isg.mdms.model.constraints.Constraint]])
  * ConstraintCollection JOIN ConstraintCollection => [[UnJoinedConstraintCollection]]
  * UnJoinedConstraintCollection .where() => [[HalfJoinedConstraintCollection]]
  * HalfJoinedConstraintCollection .equals() => [[JoinedConstraintCollection]]
  *
  * Additionally:
  * JoinedConstraintCollection .groupBy => [[GroupedConstraintCollection]]
  */

class UnJoinedConstraintCollection[A <: Constraint, B <: Constraint](lhs: Iterable[A], rhs: Iterable[B]) {

  /**
    * First join condition to determine the left hand side key.
    * @param lhsKeyFunc Takes a Constraint and returns a key of type K.
    * @tparam K Type of the key.
    */
  def where[K](lhsKeyFunc: (A) => K): HalfJoinedConstraintCollection[A, B, K] = {
    new HalfJoinedConstraintCollection[A, B, K](lhs, rhs, lhsKeyFunc)
  }

}

class HalfJoinedConstraintCollection[A <: Constraint, B <: Constraint, K](
    lhs: Iterable[A],
    rhs: Iterable[B],
    lhsKeyFunc: (A) => K) {

  /**
    * Second join condiion to determine the right hand side key.
    * @param rhsKeyFunc Takes a Constraint and returns a key of type K.
    */
  def equalsKey(rhsKeyFunc: (B) => K): JoinedConstraintCollection[A, B] = {
    if (lhs.size < rhs.size) {
      hashJoin(lhs, rhs, lhsKeyFunc, rhsKeyFunc)
    } else {
      hashJoin(rhs, lhs, rhsKeyFunc, lhsKeyFunc).reverseKeys()
    }
  }

  /**
    * Joins two ConstraintCollections based on their key functions. A hash join approach is used.
    */
  private def hashJoin[U <: Constraint, V <: Constraint]
                      (smaller: Iterable[U], larger: Iterable[V], smallerKeyFunc: (U) => K, largerKeyFunc: (V) => K)
                      : JoinedConstraintCollection[U, V] = {

    val constraintMap = smaller.map(constraint => (smallerKeyFunc(constraint), constraint)).toMap

    val joined = larger.flatMap { constraint =>
      val key = largerKeyFunc(constraint)
      if (constraintMap.contains(key)){
        Some((constraintMap(key), constraint))
      } else {
        None
      }
    }

    new JoinedConstraintCollection[U, V](joined)
  }
}


class JoinedConstraintCollection[A <: Constraint, B <: Constraint](joined: Iterable[(A, B)]) {

  /**
    * Groups the JoinedConstraintCollection based on a passed key function.
    * @param keyFunc Takes two Constraints and returns a key of type K.
    * @tparam K Type of the group key.
    */
  def groupBy[K](keyFunc: ((A, B)) => K): GroupedConstraintCollection[A, B, K] = {
    val grouped = joined.groupBy { case (a, b) => keyFunc(a, b) }.toList
    new GroupedConstraintCollection[A, B, K](grouped)
  }

  /** Returns the number of elements after the join */
  def count: Int = {
    joined.size
  }

  /** Prints amount many tuples. */
  def show(amount: Int = 10): Unit = {
    joined.take(amount).foreach(println)
  }

  /** Prints all tuples. */
  def showAll(): Unit = {
    joined.foreach(println)
  }

  /** Reverses the tuples from (A, B) to (B, A). */
  def reverseKeys(): JoinedConstraintCollection[B, A] = {
    val reversed = joined.map { case (a, b) => (b, a) }
    new JoinedConstraintCollection(reversed)
  }

  /** Returns all tuples as an Iterable of (A, B). */
  def selectAll(): Iterable[(A, B)] = {
    joined
  }

  /** Filters the tuples based on a passed filter function. */
  def where(whereFunc: ((A, B)) => Boolean): JoinedConstraintCollection[A, B] = {
    new JoinedConstraintCollection(joined.filter(whereFunc))
  }
}


class GroupedConstraintCollection[A <: Constraint, B <: Constraint, K](grouped: Iterable[(K, Iterable[(A, B)])]) {

  private type Group = Iterable[(A, B)]

  /** Returns the number of groups. */
  def count: Int = {
    grouped.size
  }

  /** Returns the total number of Constraints. */
  def sum: Int = {
    sizePerKey().map(x => x._2).sum
  }

  /** Returns the average size of the groups. */
  def average: Double = {
    // toDouble to ensure floating point division
    sum.toDouble / count
  }

  /** Applies a left fold to the JoinedConstraintCollection. */
  def fold[T](zeroValue: T)(operator: (T, (A, B)) => T): Iterable[(K, T)] = {
    grouped.map { case (key, group) =>
      (key, group.foldLeft[T](zeroValue)(operator))
    }
  }

  /** Applies a reduce to the JoinedConstraintCollection. */
  def reduce[T >: (A,B)](operator: (T, (A, B)) => T): Iterable[(K, T)] = {
    grouped.map { case (key, group) =>
      (key, group.reduceLeft[T](operator))
    }
  }

  /** Returns the size of the group per key. */
  def sizePerKey(): Iterable[(K, Int)] = {
    grouped.map { case (key, group) => (key, group.size) }
  }

  /** Returns the groups as an iterable of (key, group) */
  def selectAll(): Iterable[(K, Group)] = {
    grouped
  }

  /** Filters the groups based on a passed filter function. */
  def where(whereFunc: ((K, Group)) => Boolean): GroupedConstraintCollection[A, B, K] = {
    new GroupedConstraintCollection(grouped.filter(whereFunc))
  }
}
