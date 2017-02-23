package de.hpi.isg.mdms.analytics

import java.io.{File, IOException}
import java.sql.DriverManager

import de.hpi.isg.mdms.analytics.ConstraintImplicits.ConstraintCollectionQueryObject
import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{InclusionDependency, ColumnStatistics}
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.rdbms.SQLiteInterface
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class ConstraintImplicitsTest extends FunSuite with BeforeAndAfterEach {

  var testDb: File = _
  var store: RDBMSMetadataStore = _
  var schema: Schema = _
  var constraintCollection: ConstraintCollection[InclusionDependency] = _

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
    val constraintCollection: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
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

/* test("allOfType returns correct Constraints") {
    assert(constraintCollection.allOfType(classOf[InclusionDependency]).size == 1)
    assert(constraintCollection.allOfType(classOf[ColumnStatistics]).isEmpty)

    TestUtil.addDummyInclusionDependency(constraintCollection[InclusionDependency])
    assert(constraintCollection.allOfType(classOf[InclusionDependency]).size == 2)
  }
*/
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

  test("Join reached all intermediate representations") {
    val (indCollection, csCollection) = TestUtil.basicINDJoinCSSetup(store, schema)

    val unJoined = indCollection.join[InclusionDependency, ColumnStatistics](csCollection)
    assert(unJoined.isInstanceOf[UnJoinedConstraintCollection[InclusionDependency, ColumnStatistics]])

    val halfJoined = unJoined.where(_.getReferencedColumnIds.head)
    assert(halfJoined.isInstanceOf[HalfJoinedConstraintCollection[InclusionDependency, ColumnStatistics, Int]])

    val fullyJoined = halfJoined.equalsKey(_.getColumnId)
    assert(fullyJoined.isInstanceOf[JoinedConstraintCollection[InclusionDependency, ColumnStatistics]])

    val joined = fullyJoined.selectAll()
    assert(joined.isInstanceOf[Iterable[(InclusionDependency, ColumnStatistics)]])
  }

  test("Basic join works") {
    val (indCollection, csCollection) = TestUtil.basicINDJoinCSSetup(store, schema)

    val joined = indCollection.join[InclusionDependency, ColumnStatistics](csCollection)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getColumnId)
      .selectAll()

    val ind = indCollection.asType[InclusionDependency].head
    val cs = csCollection.asType[ColumnStatistics].head
    val expected = List((ind, cs))
    assert(joined == expected)

    val otherJoined = csCollection.join[ColumnStatistics, InclusionDependency](indCollection)
      .where(_.getColumnId)
      .equalsKey(_.getReferencedColumnIds.head)
      .selectAll()

    val otherExpected = List((cs, ind))
    assert(otherJoined == otherExpected)
  }

  test("Join matched multiple right sides") {
    val (indCollection, csCollection) = TestUtil.basicINDJoinCSSetup(store, schema)
    csCollection.add(new ColumnStatistics(0))

    val joined = indCollection.join[InclusionDependency, ColumnStatistics](csCollection)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getColumnId)
      .selectAll()

    val inds = indCollection.asType[InclusionDependency]
    val css = csCollection.asType[ColumnStatistics]

    val firstTuple = (inds.head, css.head)
    val secondTuple = (inds.head, css.last)

    val expected = List(firstTuple, secondTuple)
    assert(joined == expected)
  }

  test("Empty collections should return empty joins") {
    val empty1: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
    val empty2: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)

    val joined = empty1.join[InclusionDependency, InclusionDependency](empty2)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getReferencedColumnIds.head)
      .selectAll()

    assert(joined.isEmpty)
  }

  test("Empty and non-empty collections should return empty join") {
    val empty: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
    val nonEmpty: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
    TestUtil.addDummyInclusionDependency(nonEmpty)

    val emptyJoinedFull = empty.join[InclusionDependency, InclusionDependency](nonEmpty)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getReferencedColumnIds.head)
      .selectAll()

    assert(emptyJoinedFull.isEmpty)

    val fullJoinedEmpty = nonEmpty.join[InclusionDependency, InclusionDependency](empty)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getReferencedColumnIds.head)
      .selectAll()

    assert(fullJoinedEmpty.isEmpty)
  }

  test("Multiple entry collections should join correctly") {
    val indCollection: ConstraintCollection[InclusionDependency] = TestUtil.emptyConstraintCollection(store, schema)
    val ind01 = TestUtil.addInclusionDependency(Array(0), Array(1), indCollection)
    val ind10 = TestUtil.addInclusionDependency(Array(1), Array(0), indCollection)
    val ind02 = TestUtil.addInclusionDependency(Array(0), Array(2), indCollection)
    val ind24 = TestUtil.addInclusionDependency(Array(2), Array(4), indCollection)

    val csCollection: ConstraintCollection[ColumnStatistics] = TestUtil.emptyConstraintCollection(store, schema)
    val cs0 = new ColumnStatistics(0)
    val cs1 = new ColumnStatistics(1)
    val cs2 = new ColumnStatistics(2)
    val cs3 = new ColumnStatistics(3)
    csCollection.add(cs0)
    csCollection.add(cs1)
    csCollection.add(cs2)
    csCollection.add(cs3)

    val joined = indCollection.join[InclusionDependency, ColumnStatistics](csCollection)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getColumnId)
      .selectAll()

    val expected = List((ind01, cs1), (ind10, cs0), (ind02, cs2))

    assert(joined == expected)
  }

  test("Where on joined set returns only wanted entries") {
    val (indCollection, csCollection) = TestUtil.basicINDJoinCSSetup(store, schema)
    val ind23 = TestUtil.addInclusionDependency(Array(2), Array(3), indCollection)

    val cs3 = new ColumnStatistics(3)
    csCollection.add(cs3)

    val fullyJoined = indCollection.join[InclusionDependency, ColumnStatistics](csCollection)
      .where(_.getReferencedColumnIds.head)
      .equalsKey(_.getColumnId)

    val filtered = fullyJoined.where(_._2.getColumnId == 3).selectAll()
    val expected = List((ind23, cs3))

    assert(filtered == expected)
  }
}
