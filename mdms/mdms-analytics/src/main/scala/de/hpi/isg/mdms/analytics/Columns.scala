package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints._
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.targets.{Column, Schema}
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction

import scala.collection.JavaConversions._

/**
  * This object provides access to analytics based on [[Column]]s.
  */
object Columns {

  def findSubsetFields(schema: Schema,
                       qGramSketches: ConstraintCollection[Vector],
                       qGramSignatures: ConstraintCollection[Signature],
                       minResemblance: Double = 0.5,
                       minInclusion: Double = 0.5)
                      (implicit store: MetadataStore,
                       planBuilder: PlanBuilder) = {

    // Find out column pairs with a high q-gram resemblance.
    store.loadConstraints(qGramSignatures).cartesian(store.loadConstraints(qGramSignatures))
      .map { pair =>
        val (a, b) = (pair.field0, pair.field1)
        (a.getColumnId, b.getColumnId, calculateQGramResemblance(a, b), calculateQGramInclusion(a, b))
      }
      .filter { case (_, _, resemblance, inclusion) => resemblance >= minResemblance && inclusion > minInclusion }
  }

  def calculateQGramResemblance(a: Signature, b: Signature): Double = {
    require(a.getValues.length == b.getValues.length)
    var counter: Int = 0
    for (i <- a.getValues.indices) {
      if (a.getValues()(i) == b.getValues()(i)) counter += 1
    }
    counter.toDouble / a.getValues.length
  }

  def calculateQGramInclusion(a: Signature, b: Signature): Double = {
    require(a.getValues.length == b.getValues.length)
    var counter: Int = 0
    for (i <- a.getValues.indices) {
      if (a.getValues()(i) < b.getValues()(i)) counter += 1
    }
    counter.toDouble / a.getValues.length
  }

  def clusterVectors(vectors: ConstraintCollection[Vector],
                     k: Int,
                     iterations: Int = 50)
                    (implicit store: MetadataStore,
                     planBuilder: PlanBuilder):
  DataQuanta[ColumnClusterAssignment] = {

    // Create initial centroids by sampling input vectors.
    val initialCentroids = store.loadConstraints(vectors)
      .map(_.getValues)
      .collect()
      .take(k)
      .zipWithIndex
      .map(_.swap)

    // Do the k-means loop.
    val data = store.loadConstraints(vectors)
    val finalCentroids = planBuilder.loadCollection(initialCentroids)
      .repeat(iterations, { centroids =>
        data.mapJava(new FindClosestCentroid).withBroadcast(centroids, "centroids")
          .map { case (cluster, vector) => (cluster, vector.getValues, 1) }
          .reduceByKey(_._1, { case ((cluster, v1, n1), (_, v2, n2)) =>
            (cluster, addVectors(v1, v2), n1 + n2)
          })
          .map { case (cluster, v, n) => (cluster, divideScalar(v, n)) }
      })

    // Get the final cluster assignment.
    data.mapJava(new FindClosestCentroid).withBroadcast(finalCentroids, "centroids")
      .map { case (cluster, vector) => ColumnClusterAssignment(vector.getColumnId, cluster) }
  }

  private[analytics] class FindClosestCentroid extends ExtendedSerializableFunction[Vector, (Int, Vector)] {

    private var centroids: Iterable[(Int, Array[Double])] = _

    override def open(ctx: ExecutionContext): Unit = {
      centroids = ctx.getBroadcast[(Int, Array[Double])]("centroids")
    }

    override def apply(vector: Vector): (Int, Vector) = {
      var closestCentroid = -1
      var minDistance = Double.NaN
      for (centroid <- centroids) {
        val distance = cosineDistance(vector.getValues, centroid._2)
        if (java.lang.Double.isNaN(minDistance) || distance < minDistance) {
          minDistance = distance
          closestCentroid = centroid._1
        }
      }
      (closestCentroid, vector)
    }
  }

  def cosineDistance(a: Array[Double], b: Array[Double]) = {
    require(a.length == b.length)
    var a2, b2, ab = 0d
    a.indices foreach { i =>
      a2 += a(i) * a(i)
      b2 += b(i) * b(i)
      ab += a(i) * b(i)
    }
    if (a2 == 0 || b2 == 0) 1d
    else ab / math.sqrt(a2 * b2)
  }

  def addVectors(a: Array[Double], b: Array[Double]) = {
    require(a.length == b.length)
    val r = new Array[Double](a.length)
    a.indices foreach { i =>
      r(i) = a(i) + b(i)
    }
    r
  }

  def divideScalar(a: Array[Double], scalar: Double) = a.map(_ / scalar)

}

case class ColumnClusterAssignment(columnId: Int, clusterId: Int)
