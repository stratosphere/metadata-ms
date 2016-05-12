package de.hpi.isg.mdms.analytics.util

import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{InMemoryConstraintCollection, InclusionDependency}
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema

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

  def addInclusionDependency(referenced: Seq[Int], dependent: Seq[Int], constraintCollection: ConstraintCollection):
    InclusionDependency = {

    val reference = new InclusionDependency.Reference(referenced.toArray, dependent.toArray)
    InclusionDependency.buildAndAddToCollection(reference, constraintCollection)
  }

  def addDummyInclusionDependency(constraintCollection: ConstraintCollection): InclusionDependency = {
    addInclusionDependency(Array(0), Array(1), constraintCollection)
  }

  def emptyConstraintCollection(store: RDBMSMetadataStore, schema: Schema): ConstraintCollection = {
    new InMemoryConstraintCollection(store, schema)
  }
}
