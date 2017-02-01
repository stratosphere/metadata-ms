package de.hpi.isg.mdms.analytics.util

import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InMemoryConstraintCollection, InclusionDependency}
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.analytics.ConstraintImplicits._

import scala.reflect.ClassTag

object TestUtil {

  def fillTestDB(store: RDBMSMetadataStore, schema: Schema, constraintCollection: ConstraintCollection[InclusionDependency]): Unit = {
    val table = schema.addTable(store, "TestTable", "", new DefaultLocation)
    val columns = (0 to 4).map(c => table.addColumn(store, c.toString, "", c))

    val referenced = Array(columns.head.getId)
    val dependent = Array(columns(1).getId)
    val reference = new InclusionDependency.Reference(dependent, referenced)
    InclusionDependency.buildAndAddToCollection(reference, constraintCollection)
    store.flush()
  }

  def addInclusionDependency(dependent: Seq[Int], referenced: Seq[Int], constraintCollection: ConstraintCollection[InclusionDependency]):
    InclusionDependency = {

    val reference = new InclusionDependency.Reference(dependent.toArray, referenced.toArray)
    InclusionDependency.buildAndAddToCollection(reference, constraintCollection)
  }

  def addDummyInclusionDependency(constraintCollection: ConstraintCollection[InclusionDependency]): InclusionDependency = {
    addInclusionDependency(Array(1), Array(0), constraintCollection)
  }

  def emptyConstraintCollection[T <: Constraint : ClassTag](store: RDBMSMetadataStore, schema: Schema): ConstraintCollection[T] = {
    val classTag = implicitly[ClassTag[T]]
    val constraintClass = classTag.runtimeClass.asInstanceOf[Class[T]]
    new InMemoryConstraintCollection(store, schema, constraintClass)
  }

  def basicINDJoinCSSetup(store: RDBMSMetadataStore, schema: Schema): (ConstraintCollection[InclusionDependency], ConstraintCollection[ColumnStatistics]) = {
    val indCollection: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
    // Adds IND wit dependent: 1, referenced: 0
    TestUtil.addDummyInclusionDependency(indCollection)

    val csCollection: ConstraintCollection[ColumnStatistics] = TestUtil.emptyConstraintCollection(store, schema)
    // 0 for referenced column in IND
    csCollection.add(new ColumnStatistics(0))

    (indCollection, csCollection)
  }
}
