package de.hpi.isg.mdms.analytics.util

import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InMemoryConstraintCollection, InclusionDependency}
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.analytics.ConstraintImplicits._

object TestUtil {

  def fillTestDB(store: RDBMSMetadataStore, schema: Schema, constraintCollection: ConstraintCollection): Unit = {
    val table = schema.addTable(store, "TestTable", "", new DefaultLocation)
    val columns = (0 to 4).map(c => table.addColumn(store, c.toString, "", c))

    val referenced = Array(columns.head.getId)
    val dependent = Array(columns(1).getId)
    val reference = new InclusionDependency.Reference(dependent, referenced)
    InclusionDependency.buildAndAddToCollection(reference, constraintCollection)
    store.flush()
  }

  def addInclusionDependency(dependent: Seq[Int], referenced: Seq[Int], constraintCollection: ConstraintCollection):
    InclusionDependency = {

    val reference = new InclusionDependency.Reference(dependent.toArray, referenced.toArray)
    InclusionDependency.buildAndAddToCollection(reference, constraintCollection)
  }

  def addDummyInclusionDependency(constraintCollection: ConstraintCollection): InclusionDependency = {
    addInclusionDependency(Array(1), Array(0), constraintCollection)
  }

  def emptyConstraintCollection(store: RDBMSMetadataStore, schema: Schema): ConstraintCollection = {
    new InMemoryConstraintCollection(store, schema)
  }

  def basicINDJoinCSSetup(store: RDBMSMetadataStore, schema: Schema): (ConstraintCollection, ConstraintCollection) = {
    val indCollection = TestUtil.emptyConstraintCollection(store, schema)
    // Adds IND wit dependent: 1, referenced: 0
    TestUtil.addDummyInclusionDependency(indCollection)

    val csCollection = TestUtil.emptyConstraintCollection(store, schema)
    // 0 for referenced column in IND
    csCollection.add(new ColumnStatistics(0))

    (indCollection, csCollection)
  }
}
