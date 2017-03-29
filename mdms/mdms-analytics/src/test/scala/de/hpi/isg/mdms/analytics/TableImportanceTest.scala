package de.hpi.isg.mdms.analytics

import de.hpi.isg.mdms.model.util.IdUtils
import java.io.{File, IOException}
import java.sql.DriverManager

import de.hpi.isg.mdms.analytics.util.TestUtil
import de.hpi.isg.mdms.domain.RDBMSMetadataStore
import de.hpi.isg.mdms.domain.constraints.{ColumnStatistics, InclusionDependency, TupleCount}
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.Schema
import de.hpi.isg.mdms.rdbms.SQLiteInterface
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import de.hpi.isg.mdms.analytics
import org.qcri.rheem.api.DataQuanta


class TableImportanceTest extends FunSuite with BeforeAndAfterEach{
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
  test("Testing if sum of each row is 1.0") {
    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)
    val R = dummySchema.addTable(store, "R", null, new DefaultLocation)
    val A = R.addColumn(store, "A", null, 1)
    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val B = S.addColumn(store, "B", null, 2)
    val T = dummySchema.addTable(store, "T", null, new DefaultLocation)
    val C = T.addColumn(store, "C", null, 3)

    val tupleCountR = new TupleCount(R.getId, 6)
    val tupleCountS = new TupleCount(S.getId, 2)
    val tupleCountT = new TupleCount(T.getId, 4)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountR)
    tc.add(tupleCountS)
    tc.add(tupleCountT)


    val columnStatisticsA = new ColumnStatistics(A.getId)
    val columnStatisticsB = new ColumnStatistics(B.getId)
    val columnStatisticsC = new ColumnStatistics(C.getId)

    columnStatisticsA.setEntropy(0.6)
    columnStatisticsB.setEntropy(0.34)
    columnStatisticsC.setEntropy(0.1)
    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsA)
    cc.add(columnStatisticsB)
    cc.add(columnStatisticsC)

    val inclDepAB = new InclusionDependency(A.getId, B.getId)
    val inclDepBA = new InclusionDependency(B.getId, A.getId)
    val inclDepAC = new InclusionDependency(A.getId, C.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDepAB)
    id.add(inclDepAC)
    id.add(inclDepBA)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val tableImportance = new TableImportance

    val probabilityMatrix = tableImportance.apply(idUtils, cc, tc, id, store)

    val testRowSum = probabilityMatrix.reduceByKey(_._1, (a, b) => (a._1, a._1, a._3 + b._3))
      .map(t => t._3).collect().toSeq
  }

  test("Testing if prob matrix matches manually calculated one"){
    val connection = DriverManager.getConnection("jdbc:sqlite:" + testDb.toURI.getPath)
    store = RDBMSMetadataStore.createNewInstance(new SQLiteInterface(connection))
    val dummySchema = store.addSchema("PDB", null, new DefaultLocation)
    val S = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val S_Symb = S.addColumn(store, "S_Symb", null, 1)

    val T = dummySchema.addTable(store, "S", null, new DefaultLocation)
    val T_S_Symb = T.addColumn(store, "T_S_Symb", null, 2)
    val T_ID = T.addColumn(store, "T_ID", null, 2)


    val TR = dummySchema.addTable(store, "TR", null, new DefaultLocation)
    val TR_T_ID = TR.addColumn(store, "TR_T_ID", null, 3)
    val TR_S_Symb = TR.addColumn(store, "TR_S_Symb", null, 3)

    val tupleCountS = new TupleCount(S.getId, 6)
    val tupleCountT = new TupleCount(T.getId, 2)
    val tupleCountTR = new TupleCount(TR.getId, 4)
    val tc = store.createConstraintCollection(null, classOf[TupleCount], dummySchema)
    tc.add(tupleCountS)
    tc.add(tupleCountT)
    tc.add(tupleCountTR)

    val columnStatisticsS_Symb = new ColumnStatistics(S_Symb.getId)
    val columnStatisticsT_S_Symb = new ColumnStatistics(T_S_Symb.getId)
    val columnStatisticsT_ID = new ColumnStatistics(T_ID.getId)
    val columnStatisticsTR_T_ID = new ColumnStatistics(TR_T_ID.getId)
    val columnStatisticsTR_S_Symb = new ColumnStatistics(TR_S_Symb.getId)
    columnStatisticsS_Symb.setEntropy(0.6)
    columnStatisticsT_S_Symb.setEntropy(0.6)
    columnStatisticsT_ID.setEntropy(0.6)
    columnStatisticsTR_T_ID.setEntropy(0.6)
    columnStatisticsTR_S_Symb.setEntropy(0.6)
    val cc = store.createConstraintCollection(null, classOf[ColumnStatistics], dummySchema)
    cc.add(columnStatisticsS_Symb)
    cc.add(columnStatisticsT_S_Symb)
    cc.add(columnStatisticsT_ID)
    cc.add(columnStatisticsTR_T_ID)
    cc.add(columnStatisticsTR_S_Symb)

    val inclDep_S_Symb_T_S_Symb = new InclusionDependency(S_Symb.getId, T_S_Symb.getId)
    val inclDep_S_Symb_TR_S_Symb = new InclusionDependency(S_Symb.getId, TR_S_Symb.getId)
    val inclDep_T_ID_TR_T_ID = new InclusionDependency(T_ID.getId, TR_T_ID.getId)
    val id = store.createConstraintCollection(null, classOf[InclusionDependency], dummySchema)
    id.add(inclDep_S_Symb_T_S_Symb)
    id.add(inclDep_S_Symb_TR_S_Symb)
    id.add(inclDep_T_ID_TR_T_ID)

    val idUtils = new IdUtils(IdUtils.DEFAULT_NUM_TABLE_BITS, IdUtils.DEFAULT_NUM_COLUMN_BITS)

    val tableImportance = new TableImportance

    val probabilityMatrix = tableImportance.apply(idUtils, cc, tc, id, store)

    probabilityMatrix

  }

}


