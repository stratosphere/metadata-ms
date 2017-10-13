package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.domain.constraints.{Vector => QGramSketch}
import de.hpi.isg.mdms.model.DefaultMetadataStore
import de.hpi.isg.mdms.model.location.DefaultLocation
import org.junit.{Assert, Test}
import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.RheemContext
import org.qcri.rheem.java.Java

/**
  * Test suite for [[ColumnSimiliarity]].
  */
class ColumnSimilarityTest {

  @Test
  def testWithQGramSketches(): Unit = {
    // Create a schema.
    val mds = new DefaultMetadataStore
    val s1 = mds.addSchema("s1", "", new DefaultLocation)
    val s1t1 = s1.addTable(mds, "t1", "", new DefaultLocation)
    val s1t1c1 = s1t1.addColumn(mds, "c1", "", 0)
    val s1t1c2 = s1t1.addColumn(mds, "c2", "", 1)
    val s1t1c3 = s1t1.addColumn(mds, "c3", "", 2)
    val s1t1c4 = s1t1.addColumn(mds, "c4", "", 3)
    val s1t1c5 = s1t1.addColumn(mds, "c5", "", 4)

    // Add q-gram sketches.
    val cc = mds.createConstraintCollection("Q-gram sketches", classOf[QGramSketch], s1)
    cc.add(new QGramSketch(s1t1c1.getId, Array(0.92, -2.1, -0.8, -0.3, -0.25, -0.77, 1.01, -0.2, -0.51, 1.04)))
    cc.add(new QGramSketch(s1t1c2.getId, Array(-1.09, -1.77, -0.7, -0.88, -1.13, 0.94, -0.42, -1.76, 0.09, -1.08)))
    cc.add(new QGramSketch(s1t1c3.getId, Array(0.15, -2.03, -0.16, -0.4, -0.33, 1.44, 0.67, 0.58, 0.78, 0.55)))
    cc.add(new QGramSketch(s1t1c4.getId, Array(-1.95, 1.43, 0.64, 0.02, -0.42, -1.28, 0.57, 0.22, 0.61, 1.08)))
    cc.add(new QGramSketch(s1t1c5.getId, Array(-1.95, 1.43, 0.64, 0.02, -0.42, -1.28, 0.57, 0.22, 0.61, 1.08)))

    val rheemContext = new RheemContext().withPlugin(Java.basicPlugin)
    implicit val planBuilder = new PlanBuilder(rheemContext)
    val similarities = ColumnSimiliarity.calculateDistancesWithQGramSketches(mds, cc, s1).collect()

    Assert.assertEquals(10, similarities.size)
    similarities.find(cs => cs.columnId1 == s1t1c4.getId && cs.columnId2 == s1t1c5.getId) match {
      case None => Assert.fail("Similarities missing")
      case Some(ColumnDistance(_, _, sim)) => Assert.assertEquals(0d, sim, 0d)
    }
    similarities.find(cs => cs.columnId1 == s1t1c2.getId && cs.columnId2 == s1t1c5.getId) match {
      case None => Assert.fail("Similarities missing")
      case Some(ColumnDistance(_, _, sim)) => Assert.assertTrue(sim > 0)
    }

    similarities.foreach(cs => Assert.assertTrue(cs.value >= 0d))
    similarities.foreach(cs => Assert.assertTrue(cs.columnId1 < cs.columnId2))
  }

}
