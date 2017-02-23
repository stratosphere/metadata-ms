package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.model.DefaultMetadataStore
import org.junit.{Assert, Test}
import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.java.Java

import scala.collection.JavaConversions._

/**
  * Tests the [[MetadataStoreRheemWrapper]] class.
  */
class MetadataQuantaTest {

  private def createPlanBuilder = new PlanBuilder(new RheemContext().withPlugin(Java.basicPlugin))

  @Test
  def shouldReplaceColumnsProperly(): Unit = {
    val (store, ccFd1, ccFd2, ccUcc1, ccUcc2) = TestUtil.metadataStoreFixture1
    implicit val planBuilder = this.createPlanBuilder

    val fds = store
      .loadConstraints(ccFd1)
      .resolveColumnIds(store, fd => fd.getRhsColumnId, { case (fd, column) => (fd.getLhsColumnIds, column.nameWithSchema) })
      .resolveColumnIds(store, fd => fd._1.apply(0), { case (fd, column) => (column.nameWithSchema, fd._2) })
      .collect().toSet

    val expectedFds = Set(("schema1.table1.column1", "schema1.table1.column4"), ("schema1.table1.column0", "schema1.table1.column4"))

    Assert.assertEquals(expectedFds, fds)
  }

  @Test
  def shouldReplaceTablesProperly(): Unit = {
    val store = new DefaultMetadataStore()
    TestUtil.addSchemata(store, 2, 5, 10)

    implicit val planBuilder = this.createPlanBuilder
    val resolvedTables = planBuilder
      .loadCollection(Seq(store.getTableByName("schema0.table3"), store.getTableByName("schema1.table4")).map(_.getId))
      .resolveTableIds(store, id => id, { case (_, table) => table.nameWithSchema })
      .collect().toSet
    val expectedTables = Set("schema0.table3", "schema1.table4")

    Assert.assertEquals(expectedTables, resolvedTables)
  }

  @Test
  def shouldReplaceSchemataProperly(): Unit = {
    val store = new DefaultMetadataStore()
    TestUtil.addSchemata(store, 5, 5, 5)

    implicit val planBuilder = this.createPlanBuilder
    val resolvedSchemata = planBuilder
      .loadCollection(Seq(store.getSchemaByName("schema0"), store.getSchemaByName("schema4")).map(_.getId))
      .resolveSchemaIds(store, id => id, { case (_, schema) => schema.name })
      .collect().toSet
    val expectedSchemata = Set("schema0", "schema4")

    Assert.assertEquals(expectedSchemata, resolvedSchemata)
  }

  @Test
  def shouldCreateNewConstraintCollectionsProperly(): Unit = {
    val store = new DefaultMetadataStore()
    TestUtil.addSchemata(store, 5, 5, 5)

    implicit val planBuilder = this.createPlanBuilder
    val novelCC = planBuilder
      .loadCollection(for (i <- 0 to 100) yield i)
      .map(i => f"$i%03d")
      .storeConstraintCollection(
        store,
        Seq(store.getTableByName("schema0.table1")),
        description = "My description"
      )

    Assert.assertEquals(classOf[String], novelCC.getConstraintClass)
    Assert.assertEquals(Set(store.getTableByName("schema0.table1")), novelCC.getScope.toSet)
    Assert.assertEquals("My description", novelCC.getDescription)
    Assert.assertEquals(novelCC, store.getConstraintCollection(novelCC.getId))
  }

}
