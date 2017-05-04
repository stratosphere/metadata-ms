package de.hpi.isg.mdms.analytics

import java.io.{File, IOException}
import java.sql.DriverManager

import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.model.util.IdUtils
import de.hpi.isg.mdms.rdbms.SQLiteInterface
import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{RheemContext, _}
import org.qcri.rheem.java.Java
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class TableImportanceTest extends FunSuite with BeforeAndAfterEach {
  val rheemConfig = new Configuration
  val rheemCtx = new RheemContext(rheemConfig).withPlugin(Java.basicPlugin).withPlugin(Java.graphPlugin)
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

  test("Testing probability matrix") {
    implicit val planBuilder = new PlanBuilder(rheemCtx)
    val tableImportance = new TableImportance

    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)

    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val Sa = S.addColumn(store, "Sa", null, 1)

    val T = dummySchema.addTable(store, "T", null, new DefaultLocation)
    val T_Sa = T.addColumn(store, "T_Sa", null, 2)
    val Tb = T.addColumn(store, "Tb", null, 3)

    val U = dummySchema.addTable(store, "U", null, new DefaultLocation)
    val U_Tb = U.addColumn(store, "U_Tb", null, 4)
    val U_Sa = U.addColumn(store, "U_Sa", null, 5)

    val numTupleS = 6
    val numTupleT = 2
    val numTupleU = 4
    val tupleCountS = new TupleCount(S.getId, numTupleS)
    val tupleCountT = new TupleCount(T.getId, numTupleT)
    val tupleCountU = new TupleCount(U.getId, numTupleU)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountS)
    tc.add(tupleCountT)
    tc.add(tupleCountU)

    val columnStatisticsSa = new ColumnStatistics(Sa.getId)
    val columnStatisticsT_Sa = new ColumnStatistics(T_Sa.getId)
    val columnStatisticsTb = new ColumnStatistics(Tb.getId)
    val columnStatisticsU_Tb = new ColumnStatistics(U_Tb.getId)
    val columnStatisticsU_Sa = new ColumnStatistics(U_Sa.getId)
    val ent_Sa = 0.6
    val ent_T_Sa = 0.6
    val ent_Tb = 0.3
    val ent_U_Tb = 0.1
    val ent_U_Sa = 0.7
    columnStatisticsSa.setEntropy(ent_Sa)
    columnStatisticsT_Sa.setEntropy(ent_T_Sa)
    columnStatisticsTb.setEntropy(ent_Tb)
    columnStatisticsU_Tb.setEntropy(ent_U_Tb)
    columnStatisticsU_Sa.setEntropy(ent_U_Sa)
    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsSa)
    cc.add(columnStatisticsT_Sa)
    cc.add(columnStatisticsTb)
    cc.add(columnStatisticsU_Tb)
    cc.add(columnStatisticsU_Sa)

    val inclDep_Sa_TSa = new InclusionDependency(Sa.getId, T_Sa.getId)
    val inclDep_Sa_USa = new InclusionDependency(Sa.getId, U_Sa.getId)
    val inclDep_Tb_UTb = new InclusionDependency(Tb.getId, U_Tb.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDep_Sa_TSa)
    id.add(inclDep_Sa_USa)
    id.add(inclDep_Tb_UTb)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val probabilityMatrix = tableImportance.probMatrix(idUtils, cc, tc, id, store)
      .map(t => t._3).collect()

    // Calculate probability matrix manually
    val P_Sa_TSa = ent_Sa / (math.log(numTupleS) + 2 * ent_Sa)
    val P_Sa_USa = ent_Sa / (math.log(numTupleS) + 2 * ent_Sa)
    val P_Tb_UTb = ent_Tb / (math.log(numTupleT) + ent_Tb)

    val PI_S_T = P_Sa_TSa
    val PI_S_U = P_Sa_USa
    val PI_T_U = P_Tb_UTb
    val PI_S_S = 1 - PI_S_T - PI_S_U
    val PI_T_T = 1 - PI_T_U
    val PI_U_U = 1 - 0

    probabilityMatrix.foreach(println)

    // testing if matrix calculates manually one
    assert(Seq(PI_U_U, PI_S_S, PI_T_T, PI_T_U, PI_S_T, PI_S_U) == probabilityMatrix)

    // testing if rows sum up to 1
    val testRowSum = tableImportance.probMatrix(idUtils, cc, tc, id, store)
      .reduceByKey(_._1, (a, b) => (a._1, a._1, a._3 + b._3))
      .map(t => t._3).collect().toSeq
    assert(Seq(1.0, 1.0, 1.0) == testRowSum)
  }

  test("Testing table importance") {
    implicit val planBuilder = new PlanBuilder(rheemCtx)
    val tableImportance = new TableImportance

    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)

    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val Sa = S.addColumn(store, "Sa", null, 1)

    val T = dummySchema.addTable(store, "T", null, new DefaultLocation)
    val T_Sa = T.addColumn(store, "T_Sa", null, 2)
    val Tb = T.addColumn(store, "Tb", null, 3)

    val U = dummySchema.addTable(store, "U", null, new DefaultLocation)
    val U_Tb = U.addColumn(store, "U_Tb", null, 4)
    val U_Sa = U.addColumn(store, "U_Sa", null, 5)

    val numTupleS = 6
    val numTupleT = 2
    val numTupleU = 4
    val tupleCountS = new TupleCount(S.getId, numTupleS)
    val tupleCountT = new TupleCount(T.getId, numTupleT)
    val tupleCountU = new TupleCount(U.getId, numTupleU)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountS)
    tc.add(tupleCountT)
    tc.add(tupleCountU)

    val columnStatisticsSa = new ColumnStatistics(Sa.getId)
    val columnStatisticsT_Sa = new ColumnStatistics(T_Sa.getId)
    val columnStatisticsTb = new ColumnStatistics(Tb.getId)
    val columnStatisticsU_Tb = new ColumnStatistics(U_Tb.getId)
    val columnStatisticsU_Sa = new ColumnStatistics(U_Sa.getId)
    val ent_Sa = 0.6
    val ent_T_Sa = 0.6
    val ent_Tb = 0.3
    val ent_U_Tb = 0.1
    val ent_U_Sa = 0.7
    columnStatisticsSa.setEntropy(ent_Sa)
    columnStatisticsT_Sa.setEntropy(ent_T_Sa)
    columnStatisticsTb.setEntropy(ent_Tb)
    columnStatisticsU_Tb.setEntropy(ent_U_Tb)
    columnStatisticsU_Sa.setEntropy(ent_U_Sa)
    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsSa)
    cc.add(columnStatisticsT_Sa)
    cc.add(columnStatisticsTb)
    cc.add(columnStatisticsU_Tb)
    cc.add(columnStatisticsU_Sa)

    val inclDep_Sa_TSa = new InclusionDependency(Sa.getId, T_Sa.getId)
    val inclDep_Sa_USa = new InclusionDependency(Sa.getId, U_Sa.getId)
    val inclDep_Tb_UTb = new InclusionDependency(Tb.getId, U_Tb.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDep_Sa_TSa)
    id.add(inclDep_Sa_USa)
    id.add(inclDep_Tb_UTb)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val probabilityMatrix = tableImportance.probMatrix(idUtils, cc, tc, id, store)

    val V = tableImportance.tableImport(probabilityMatrix, tc, store, idUtils, cc,
      maxnumOrEpsilon = false, numIteration = 1000, epsilon = 1e-10)
      .map(t => t._2).collect().toList

    // Test ranking of table importance
    // 1st rank: U
    // 2nd rank: T
    // 3rd rand: S
    assert(V(2) > V(1) && V(0) > V(1))
  }

  test("Testing if importance flows from left to right") {
    implicit val planBuilder = new PlanBuilder(rheemCtx)
    val tableImportance = new TableImportance

    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)

    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val Sa = S.addColumn(store, "Sa", null, 1)

    val T = dummySchema.addTable(store, "T", null, new DefaultLocation)
    val Ta = T.addColumn(store, "Ta", null, 2)

    val numTupleS = 6
    val numTupleT = 6
    val tupleCountS = new TupleCount(S.getId, numTupleS)
    val tupleCountT = new TupleCount(T.getId, numTupleT)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountS)
    tc.add(tupleCountT)

    val columnStatisticsSa = new ColumnStatistics(Sa.getId)
    val columnStatisticsTa = new ColumnStatistics(Ta.getId)

    val ent_Sa = 10
    val ent_Ta = 0.5
    columnStatisticsSa.setEntropy(ent_Sa)
    columnStatisticsTa.setEntropy(ent_Ta)

    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsSa)
    cc.add(columnStatisticsTa)

    val inclDep1 = new InclusionDependency(Sa.getId, Ta.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDep1)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val probabilityMatrix = tableImportance.probMatrix(idUtils, cc, tc, id, store)


    val V = tableImportance.tableImport(probabilityMatrix, tc, store, idUtils, cc,
      maxnumOrEpsilon = false, numIteration = 1000, epsilon = 1e-10)
      .map(t => t._2).collect().toList

    assert(V(0) < 1e-10 && V(1) > 11.99999)
  }


  test("Testing if importance flows from right to left") {
    implicit val planBuilder = new PlanBuilder(rheemCtx)
    val tableImportance = new TableImportance

    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)

    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val Sa = S.addColumn(store, "Sa", null, 1)

    val T = dummySchema.addTable(store, "T", null, new DefaultLocation)
    val Ta = T.addColumn(store, "Ta", null, 2)

    val numTupleS = 6
    val numTupleT = 6
    val tupleCountS = new TupleCount(S.getId, numTupleS)
    val tupleCountT = new TupleCount(T.getId, numTupleT)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountS)
    tc.add(tupleCountT)

    val columnStatisticsSa = new ColumnStatistics(Sa.getId)
    val columnStatisticsTa = new ColumnStatistics(Ta.getId)

    val ent_Sa = 10
    val ent_Ta = 0.5
    columnStatisticsSa.setEntropy(ent_Sa)
    columnStatisticsTa.setEntropy(ent_Ta)

    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsSa)
    cc.add(columnStatisticsTa)

    val inclDep1 = new InclusionDependency(Ta.getId, Sa.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDep1)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val probabilityMatrix = tableImportance.probMatrix(idUtils, cc, tc, id, store)


    val V = tableImportance.tableImport(probabilityMatrix, tc, store, idUtils, cc,
      maxnumOrEpsilon = false, numIteration = 100, epsilon = 1e-10)
      .map(t => t._2).collect().toList

    assert(V(0) < 1e-9 && V(1) > 11.99999)
  }
}
