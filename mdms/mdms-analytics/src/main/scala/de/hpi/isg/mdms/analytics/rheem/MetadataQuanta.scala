package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.experiment.Experiment
import de.hpi.isg.mdms.model.targets.{Column, Schema, Table, Target}
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}

import scala.reflect.ClassTag

/**
  * This class pimps Rheem's [[DataQuanta]] with additional operations.
  */
class MetadataQuanta[Out: ClassTag](dataQuanta: DataQuanta[Out]) {

  /**
    * Resolve some [[Schema]] ID in the [[DataQuanta]].
    *
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[SchemaMock]]
    * @param metadataStore in which the [[Schema]]s reside
    * @return [[DataQuanta]] with resolved [[Schema]] IDs
    */
  def resolveSchemaIds[NewOut: ClassTag](idExtractor: Out => Int,
                                         finisher: (Out, SchemaMock) => NewOut)
                                        (implicit metadataStore: MetadataStore):
  DataQuanta[NewOut] = {
    val schemata = metadataStore.loadSchemata()(dataQuanta.planBuilder)
    dataQuanta
      .join[SchemaMock, Int](idExtractor, schemata, _.id)
      .map { join => finisher(join.field0, join.field1) }
  }

  /**
    * Resolve some [[Table]] ID in the [[DataQuanta]].
    *
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[TableMock]]
    * @param metadataStore in which the [[Table]]s reside
    * @return [[DataQuanta]] with resolved [[Table]] IDs
    */
  def resolveTableIds[NewOut: ClassTag](idExtractor: Out => Int,
                                        finisher: (Out, TableMock) => NewOut)
                                       (implicit metadataStore: MetadataStore):
  DataQuanta[NewOut] = {
    val tables = metadataStore.loadTables(null)(dataQuanta.planBuilder)
    dataQuanta
      .join[TableMock, Int](idExtractor, tables, _.id)
      .map { join => finisher(join.field0, join.field1) }
  }

  /**
    * Resolve some [[Column]] ID in the [[DataQuanta]].
    *
    * @param idExtractor   extracts the ID to be replaced
    * @param finisher      creates new data quanta from the old one and its matching [[ColumnMock]]
    * @param metadataStore in which the [[Column]]s reside
    * @return [[DataQuanta]] with resolved [[Column]] IDs
    */
  def resolveColumnIds[NewOut: ClassTag](idExtractor: Out => Int,
                                         finisher: (Out, ColumnMock) => NewOut)
                                        (implicit metadataStore: MetadataStore):
  DataQuanta[NewOut] = {
    val columns = metadataStore.loadColumns(null)(dataQuanta.planBuilder)
    dataQuanta.keyBy(idExtractor).join(columns.keyBy(_.id)).assemble(finisher)
  }

  /**
    * Store the given this [[DataQuanta]] in a new [[ConstraintCollection]] within the `store`.
    *
    * @param scope         of the new [[ConstraintCollection]]
    * @param userDefinedId an optional ID to identify the [[ConstraintCollection]]
    * @param description   for the new [[ConstraintCollection]]
    * @param experiment    an optional [[Experiment]] to which the [[ConstraintCollection]] should belong
    * @param store         in which the [[DataQuanta]] should be stored
    * @return the [[ConstraintCollection]]
    */
  def storeConstraintCollection(scope: Iterable[Target],
                                userDefinedId: String = null,
                                description: String = "(no description)",
                                experiment: Experiment = null)
                               (implicit store: MetadataStore):
  ConstraintCollection[Out] = {
    val cls = implicitly[ClassTag[Out]].runtimeClass.asInstanceOf[Class[Out]]
    val cc = store.createConstraintCollection(userDefinedId, description, experiment, cls, scope.toSeq: _*)
    this.store(cc)
    cc
  }

  /**
    * Store the given this [[DataQuanta]] in the given `constraintCollection`, thereby leaving the existing
    * elements intact.
    *
    * @param constraintCollection the [[ConstraintCollection]]
    */
  def store(constraintCollection: ConstraintCollection[Out]): Unit = {
    dataQuanta.foreach((quantum: Out) => constraintCollection.add(quantum))
  }

}
