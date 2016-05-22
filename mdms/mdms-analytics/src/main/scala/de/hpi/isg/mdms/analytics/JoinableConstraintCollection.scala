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

  def where[K](lhsKeyFunc: (A) => K): HalfJoinedConstraintCollection[A, B, K] = {
    new HalfJoinedConstraintCollection[A, B, K](lhs, rhs, lhsKeyFunc)
  }

}

class HalfJoinedConstraintCollection[A <: Constraint, B <: Constraint, K](
    lhs: Iterable[A],
    rhs: Iterable[B],
    lhsKeyFunc: (A) => K) {

  def equalsKey(rhsKeyFunc: (B) => K): JoinedConstraintCollection[A, B] = {
    if (lhs.size < rhs.size) {
      hashJoin(lhs, rhs, lhsKeyFunc, rhsKeyFunc)
    } else {
      hashJoin(rhs, lhs, rhsKeyFunc, lhsKeyFunc).reverseKeys()
    }
  }

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

  def groupBy[K](keyFunc: (A, B) => K): GroupedConstraintCollection[A, B, K] = {
    val grouped = joined.groupBy { case (a, b) => keyFunc(a, b) }.toList
    new GroupedConstraintCollection[A, B, K](grouped)
  }

  def count: Int = {
    joined.size
  }

  def show(amount: Int = 10): Unit = {
    joined.take(amount).foreach(println)
  }

  def showAll(): Unit = {
    joined.foreach(println)
  }

  def reverseKeys(): JoinedConstraintCollection[B, A] = {
    val reversed = joined.map { case (a, b) => (b, a) }
    new JoinedConstraintCollection(reversed)
  }

  def selectAll(): Iterable[(A, B)] = {
    joined
  }

  def where(whereFunc: ((A, B)) => Boolean): JoinedConstraintCollection[A, B] = {
    new JoinedConstraintCollection(joined.filter(whereFunc))
  }
}


class GroupedConstraintCollection[A <: Constraint, B <: Constraint, K](grouped: Iterable[(K, Iterable[(A, B)])]) {

  private type Group = Iterable[(A, B)]

  def count: Int = {
    grouped.size
  }

  def sum: Int = {
    grouped.map { case (_, joins) => joins.size }.sum
  }

  def average: Double = {
    // toDouble to ensure floating point division
    sum.toDouble / count
  }

  def fold[T](zeroValue: T)(operator: (T, (A, B)) => T): Iterable[(K, T)] = {
    grouped.map { case (key, group) =>
      (key, group.foldLeft[T](zeroValue)(operator))
    }
  }

  def reduce[T >: (A,B)](operator: (T, (A, B)) => T): Iterable[(K, T)] = {
    grouped.map { case (key, group) =>
      (key, group.reduceLeft[T](operator))
    }
  }

  def selectAll(): Iterable[(K, Iterable[(A, B)])] = {
    grouped
  }

  def where(whereFunc: ((K, Group)) => Boolean): GroupedConstraintCollection[A, B, K] = {
    new GroupedConstraintCollection(grouped.filter(whereFunc))
  }
}
