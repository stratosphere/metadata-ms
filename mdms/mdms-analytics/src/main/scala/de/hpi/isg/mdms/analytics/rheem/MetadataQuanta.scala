package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.targets.{Column, Schema, Table}
import org.qcri.rheem.api.DataQuanta

import scala.reflect.ClassTag

/**
  * This class pimps Rheem's [[DataQuanta]] with additional operations.
  */
class MetadataQuanta[Out: ClassTag](dataQuanta: DataQuanta[Out]) {

  /**
    * Resolve some [[Schema]] ID in the [[DataQuanta]].
    *
    * @param metadataStore in which the [[Schema]]s reside
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[SchemaMock]]
    * @return [[DataQuanta]] with resolved [[Schema]] IDs
    */
  def resolveSchemaIds[NewOut: ClassTag](metadataStore: MetadataStore,
                                         idExtractor: Out => Int,
                                         finisher: (Out, SchemaMock) => NewOut): DataQuanta[NewOut] = {
    implicit val planBuilder = dataQuanta.planBuilder
    val schemata = metadataStore.loadSchemata()
    dataQuanta
      .join[SchemaMock, Int](idExtractor, schemata, _.id)
      .map { join => finisher(join.field0, join.field1) }
  }

  /**
    * Resolve some [[Table]] ID in the [[DataQuanta]].
    *
    * @param metadataStore in which the [[Table]]s reside
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[TableMock]]
    * @return [[DataQuanta]] with resolved [[Table]] IDs
    */
  def resolveTableIds[NewOut: ClassTag](metadataStore: MetadataStore,
                                        idExtractor: Out => Int,
                                        finisher: (Out, TableMock) => NewOut): DataQuanta[NewOut] = {
    implicit val planBuilder = dataQuanta.planBuilder
    val tables = metadataStore.loadTables(null)
    dataQuanta
      .join[TableMock, Int](idExtractor, tables, _.id)
      .map { join => finisher(join.field0, join.field1) }
  }

  /**
    * Resolve some [[Column]] ID in the [[DataQuanta]].
    *
    * @param metadataStore in which the [[Column]]s reside
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[ColumnMock]]
    * @return [[DataQuanta]] with resolved [[Column]] IDs
    */
  def resolveColumnIds[NewOut: ClassTag](metadataStore: MetadataStore,
                                         idExtractor: Out => Int,
                                         finisher: (Out, ColumnMock) => NewOut): DataQuanta[NewOut] = {
    implicit val planBuilder = dataQuanta.planBuilder
    val columns = metadataStore.loadColumns(null)
    dataQuanta.keyBy(idExtractor).joinAndAssemble(columns.keyBy(_.id), finisher)
  }

  /**
    * Assigns this instance a key extractor, which enables some key-based operations.
    *
    * @see KeyedDataQuanta
    * @param keyExtractor extracts the key from the [[DataQuanta]]
    * @return the [[KeyedDataQuanta]]
    */
  def keyBy[Key: ClassTag](keyExtractor: Out => Key) = new KeyedDataQuanta[Out, Key](dataQuanta, keyExtractor)

}
