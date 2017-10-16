package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.domain.constraints.{FunctionalDependency, UniqueColumnCombination}
import org.junit.{Assert, Test}
import org.mockito.Mockito
import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.java.Java
import scala.collection.JavaConversions._

/**
  * Tests the [[MetadataStoreRheemWrapper]] class.
  */
class MetadataStoreRheemWrapperTest {

  @Test
  def shouldFindConstraintCollectionsCorrectly(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1

    val s1t1 = store.getTableByName("schema1.table1")
    val foundCcFd1 = store.findConstraintCollections[FunctionalDependency](s1t1)
    Assert.assertEquals(Seq(ccFd1), foundCcFd1)
    val foundCcUcc1 = store.findConstraintCollections[UniqueColumnCombination](s1t1)
    Assert.assertEquals(Seq(ccUcc1), foundCcUcc1)

    val s1t2 = store.getTableByName("schema1.table2")
    val foundCcFd2 = store.findConstraintCollections[FunctionalDependency](s1t2)
    Assert.assertEquals(Seq(ccFd2), foundCcFd2)
    val foundCcUcc2 = store.findConstraintCollections[UniqueColumnCombination](s1t2)
    Assert.assertEquals(Seq(ccUcc2), foundCcUcc2)
  }

  @Test
  def shouldFindIncludedConstraintCollectionsCorrectly(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1

    val schema1 = store.getSchemaByName("schema1")
    Assert.assertEquals(Set(ccFd1, ccFd2), store.listConstraintCollections[FunctionalDependency](schema1).toSet)

    val table1 = store.getTableByName("schema1.table1")
    Assert.assertEquals(Set(ccFd1), store.listConstraintCollections[FunctionalDependency](table1).toSet)

    val table2 = store.getTableByName("schema1.table2")
    Assert.assertEquals(Set(ccUcc2), store.listConstraintCollections[UniqueColumnCombination](table2).toSet)
  }

  @Test(expected = classOf[ConstraintCollectionConflictException])
  def shouldFailOnConflictsByDefault(): Unit = {
    val (store, ccFd1, ccFd2) = TestUtil.metadataStoreFixture2

    val table1 = store.getTableByName("schema1.table1")
    store.findConstraintCollections[FunctionalDependency](table1)
  }

  @Test(expected = classOf[NoConstraintCollectionException])
  def shouldFailOnOnMissingConstraintCollections(): Unit = {
    val (store, ccFd1, ccFd2) = TestUtil.metadataStoreFixture2

    val table2 = store.getTableByName("schema1.table2")
    implicit val planBuilder = Mockito.mock(classOf[PlanBuilder])
    store.loadConstraints[FunctionalDependency](table2)
  }

  @Test
  def shouldLoadConstraintCollectionsByUserDefinedId(): Unit = {
    val (store, ccFd1, _) = TestUtil.metadataStoreFixture2
    val rheemContext = new RheemContext().withPlugin(Java.basicPlugin)
    implicit val planBuilder = new PlanBuilder(rheemContext)
    val fds = store.loadConstraints[FunctionalDependency]("ccFd1").collect()
    Assert.assertEquals(ccFd1.getConstraints.toSet, fds.toSet)
  }

  @Test(expected = classOf[NoConstraintCollectionException])
  def shouldFailOnOnMissingConstraintCollectionsForUserDefinedId(): Unit = {
    val (store, ccFd1, ccFd2) = TestUtil.metadataStoreFixture2

    val table2 = store.getTableByName("schema1.table2")
    implicit val planBuilder = Mockito.mock(classOf[PlanBuilder])
    store.loadConstraints[FunctionalDependency]("ccFd42")
  }

  @Test
  def testLoadColumns(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1

    val rheemContext = new RheemContext().withPlugin(Java.basicPlugin)
    implicit val planBuilder = new PlanBuilder(rheemContext)
    Assert.assertEquals(
      1L, store.loadColumns(store.getTargetByName("schema1.table1.column1")).count.collect().head
    )
    Assert.assertEquals(
      10L, store.loadColumns(store.getTargetByName("schema1.table1")).count.collect().head
    )
    Assert.assertEquals(
      2 * 5 * 10L, store.loadColumns().count.collect().head
    )
  }

  @Test
  def testLoadTables(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1

    val rheemContext = new RheemContext().withPlugin(Java.basicPlugin)
    implicit val planBuilder = new PlanBuilder(rheemContext)
    Assert.assertEquals(
      1L, store.loadTables(store.getTargetByName("schema1.table1")).count.collect().head
    )
    Assert.assertEquals(
      5L, store.loadTables(store.getTargetByName("schema1")).count.collect().head
    )
    Assert.assertEquals(
      2 * 5L, store.loadTables().count.collect().head
    )
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testLoadTablesFailsForColumns(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1

    val rheemContext = new RheemContext().withPlugin(Java.basicPlugin)
    implicit val planBuilder = new PlanBuilder(rheemContext)
    store.loadTables(store.getTargetByName("schema1.table1.column1")).collect()
  }

}
