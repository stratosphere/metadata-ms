package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.constraints.Constraint

/**
  * @author Lawrence Benson
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

  def groupBy[K](keyFunc: (A, B) => K): GroupedJoinedConstraintCollection[A, B, K] = {
    val grouped = joined.groupBy { case (a, b) => keyFunc(a, b) }.toList
    new GroupedJoinedConstraintCollection[A, B, K](grouped)
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
}


class GroupedJoinedConstraintCollection[A <: Constraint, B <: Constraint, K](grouped: Iterable[(K, Iterable[(A, B)])]) {
  
  def count: Int = {
    grouped.size
  }

  def sum: Int = {
    grouped.map { case (_, joins) => joins.size }.sum
  }

  def average: Double = {
    sum / count
  }
}
