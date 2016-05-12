package de.hpi.isg.mdms.analytics

import java.io.{File, IOException}
import java.sql.DriverManager

import de.hpi.isg.mdms.analytics.ConstraintImplicits.ConstraintCollectionQueryObject
import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InMemoryConstraintCollection, InclusionDependency}
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.rdbms.SQLiteInterface
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class ConstraintImplicitsTest extends FunSuite with BeforeAndAfterEach {

  var testDb: File = _
  var store: RDBMSMetadataStore = _
  var schema: Schema = _
  var constraintCollection: ConstraintCollection = _

  override def beforeEach(): Unit = {
    try {
      testDb = File.createTempFile("test", ".db")
      testDb.deleteOnExit()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }

    Class.forName("org.sqlite.JDBC")
    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    schema = store.addSchema("TestSchema", "", new DefaultLocation)
    constraintCollection = TestUtil.emptyConstraintCollection(store, schema)
    TestUtil.fillTestDB(store, schema, constraintCollection)
  }

  override def afterEach(): Unit = {
    store.close()
    testDb.delete()
  }

  test("An empty ConstraintCollection should return an empty Iterator") {
    val constraintCollection = TestUtil.emptyConstraintCollection(store, schema)
    assert(constraintCollection.constraintsIter.isEmpty)
  }

  test("A non-empty ConstraintCollection should return a non-empty Iterator") {
    assert(constraintCollection.constraintsIter.nonEmpty)
  }

  test("count returns the correct amount of items") {
    assert(constraintCollection.count == 1)
    (1 to 10).foreach { _ =>
      TestUtil.addDummyInclusionDependency(constraintCollection)
    }
    assert(constraintCollection.count == 11)
  }

  test("allOfType returns correct Constraints") {
    assert(constraintCollection.allOfType(classOf[InclusionDependency]).size == 1)
    assert(constraintCollection.allOfType(classOf[ColumnStatistics]).isEmpty)

    TestUtil.addDummyInclusionDependency(constraintCollection)
    assert(constraintCollection.allOfType(classOf[InclusionDependency]).size == 2)
  }

  test("asType casts, if cast is possible") {
    constraintCollection.asType[Constraint].foreach { constraint =>
      constraint.getClass == classOf[Constraint]
    }
  }

  test("asType throws an Exception, if tye not castable") {
    intercept[ClassCastException] {
      constraintCollection.asType[ColumnStatistics].head
    }
  }

  test("groupByType should return correct groups") {
    constraintCollection.add(new ColumnStatistics(0))
    val grouped = constraintCollection.groupByType()
    val expected = Map(classOf[InclusionDependency] -> List(constraintCollection.constraintsIter.head),
      classOf[ColumnStatistics] -> List(constraintCollection.constraintsIter.tail.head))
    assert(grouped == expected)
  }
}
