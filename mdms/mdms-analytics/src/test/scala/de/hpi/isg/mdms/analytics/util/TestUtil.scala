package de.hpi.isg.mdms.analytics.util

import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints._
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.{Schema, Table}
import de.hpi.isg.mdms.model.{DefaultMetadataStore, MetadataStore}

import scala.reflect.ClassTag

object TestUtil {

  def fillTestDB(store: RDBMSMetadataStore, schema: Schema, constraintCollection: ConstraintCollection[InclusionDependency]): Unit = {
    val table = schema.addTable(store, "TestTable", "", new DefaultLocation)
    val columns = (0 to 4).map(c => table.addColumn(store, c.toString, "", c))

    val referenced = Array(columns.head.getId)
    val dependent = Array(columns(1).getId)
    constraintCollection.add(new InclusionDependency(dependent, referenced))
    store.flush()
  }

  def addInclusionDependency(dependent: Seq[Int], referenced: Seq[Int], constraintCollection: ConstraintCollection[InclusionDependency]):
  InclusionDependency = {
    val ind = new InclusionDependency(dependent.toArray, referenced.toArray)
    constraintCollection.add(ind)
    ind
  }

  def addDummyInclusionDependency(constraintCollection: ConstraintCollection[InclusionDependency]): InclusionDependency = {
    addInclusionDependency(Array(1), Array(0), constraintCollection)
  }

  def emptyConstraintCollection[T <: Constraint : ClassTag](store: RDBMSMetadataStore, schema: Schema): ConstraintCollection[T] = {
    val classTag = implicitly[ClassTag[T]]
    val constraintClass = classTag.runtimeClass.asInstanceOf[Class[T]]
    new InMemoryConstraintCollection(store, null, schema, constraintClass)
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

  def metadataStoreFixture1 = {
    val store = new DefaultMetadataStore()

    // Set up the schemata.
    addSchemata(store, 2, 5, 10)

    // Add some constraints.
    val ccFd1 = store.createConstraintCollection("FDs on schema1.table1", classOf[FunctionalDependency], store.getTableByName("schema1.table1"))
    ccFd1.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column0").getId),
        store.getColumnByName("schema1.table1.column4").getId
      )
    )
    ccFd1.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column1"), store.getColumnByName("schema1.table1.column2")).map(_.getId),
        store.getColumnByName("schema1.table1.column4").getId
      )
    )

    // Add some more constraints.
    val ccFd2 = store.createConstraintCollection("FDs on schema1.table2", classOf[FunctionalDependency], store.getTableByName("schema1.table2"))
    ccFd2.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table2.column0")).map(_.getId),
        store.getColumnByName("schema1.table2.column4").getId
      )
    )
    ccFd2.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table2.column1"), store.getColumnByName("schema1.table2.column2")).map(_.getId),
        store.getColumnByName("schema1.table2.column4").getId
      )
    )

    // Add some constraints.
    val ccUcc1 = store.createConstraintCollection("UCCs on schema1.table1", classOf[UniqueColumnCombination], store.getTableByName("schema1.table1"))
    ccUcc1.add(
      new UniqueColumnCombination(
        Array(store.getColumnByName("schema1.table1.column0")).map(_.getId)
      )
    )
    ccUcc1.add(
      new UniqueColumnCombination(
        Array(store.getColumnByName("schema1.table1.column1"), store.getColumnByName("schema1.table1.column2")).map(_.getId)
      )
    )

    // Add some more constraints.
    val ccUcc2 = store.createConstraintCollection("FDs on schema1.table2", classOf[UniqueColumnCombination], store.getTableByName("schema1.table2"))
    ccUcc2.add(
      new UniqueColumnCombination(
        Array(store.getColumnByName("schema1.table2.column0")).map(_.getId)
      )
    )
    ccUcc2.add(
      new UniqueColumnCombination(
        Array(store.getColumnByName("schema1.table2.column1"), store.getColumnByName("schema1.table2.column2")).map(_.getId)
      )
    )

    (store, ccFd1, ccFd2, ccUcc1, ccUcc2)
  }

  def metadataStoreFixture2 = {
    val store = new DefaultMetadataStore()

    // Set up the schemata.
    addSchemata(store, 2, 3, 10)

    // Add some constraints.
    val ccFd1 = store.createConstraintCollection("ccFd1", "FDs on schema1.table1", null, classOf[FunctionalDependency], store.getTableByName("schema1.table1"))
    ccFd1.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column0")).map(_.getId),
        store.getColumnByName("schema1.table1.column4").getId
      )
    )
    ccFd1.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column1"), store.getColumnByName("schema1.table1.column2")).map(_.getId),
        store.getColumnByName("schema1.table1.column4").getId
      )
    )

    // Add some more constraints.
    val ccFd2 = store.createConstraintCollection("ccFd2", "FDs on schema1.table1", null, classOf[FunctionalDependency], store.getTableByName("schema1.table1"))
    ccFd2.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column0")).map(_.getId),
        store.getColumnByName("schema1.table1.column4").getId
      )
    )
    ccFd2.add(
      new FunctionalDependency(
        Array(store.getColumnByName("schema1.table1.column1"), store.getColumnByName("schema1.table1.column2")).map(_.getId),
        store.getColumnByName("schema1.table1.column2").getId
      )
    )

    (store, ccFd1, ccFd2)
  }

  def addSchemata(store: MetadataStore, numSchemata: Int, numTables: Int, numColumns: Int): Unit = {
    for (i <- 0 until numSchemata) {
      val schema = store.addSchema(s"schema$i", "A test schema", new DefaultLocation)
      addTables(store, schema, numTables, numColumns)
    }
  }

  def addTables(store: MetadataStore, schema: Schema, numTables: Int, numColumns: Int): Unit = {
    for (i <- 0 until numTables) {
      val table = schema.addTable(store, s"table$i", "A test table", new DefaultLocation)
      addColumns(store, table, numColumns)
    }
  }

  def addColumns(store: MetadataStore, table: Table, n: Int): Unit =
    for (i <- 0 until n) table.addColumn(store, s"column$i", "A test column", i)

}
