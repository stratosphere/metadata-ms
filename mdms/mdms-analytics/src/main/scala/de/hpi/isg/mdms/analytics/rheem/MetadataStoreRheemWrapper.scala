package de.hpi.isg.mdms.analytics.rheem

import de.hpi.isg.mdms.analytics._
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.targets.{Column, Schema, Table, Target}
import org.qcri.rheem.api._

import scala.collection.JavaConversions._
import scala.reflect._

/**
  * Exposes metadata from a [[de.hpi.isg.mdms.model.MetadataStore]] as [[org.qcri.rheem.api.DataQuanta]].
  */
class MetadataStoreRheemWrapper(metadataStore: MetadataStore) {

  /**
    * Retrieves all [[ConstraintCollection]]s and resolves potential conflicts if multiple ones are found. Those
    * are then delivered as [[DataQuanta]].
    *
    * @param scope                      of the [[ConstraintCollection]]s
    * @param conflictResolutionStrategy decides how to act in the presence of multiple matching [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return [[DataQuanta]] of the [[Constraint]]s in the matching, resolved [[ConstraintCollection]]s
    */
  def loadConstraints[Type: ClassTag]
  (scope: Target,
   conflictResolutionStrategy: ConstraintCollectionConflictResolutionStrategy = FailOnConflictStrategy)
  (implicit planBuilder: PlanBuilder):
  DataQuanta[Type] = {
    val constraintCollections = findConstraintCollections[Type](scope, conflictResolutionStrategy)
    if (constraintCollections.isEmpty)
      throw new NoConstraintCollectionException("Could not find the requested constraint collection.")

    loadConstraints(constraintCollections: _*)
  }

  /**
    * Retrieve a [[ConstraintCollection]]s and resolves potential conflicts if multiple ones are found.
    *
    * @param scope                      of the [[ConstraintCollection]]s
    * @param conflictResolutionStrategy decides how to act in the presence of multiple matching [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return the matching, resolved [[ConstraintCollection]]
    */
  def findConstraintCollection[Type: ClassTag]
  (scope: Target, conflictResolutionStrategy: ConstraintCollectionConflictResolutionStrategy = FailOnConflictStrategy):
  ConstraintCollection[Type] = {
    val typeClass = classTag[Type].runtimeClass.asInstanceOf[java.lang.Class[Type]]
    val constraintCollections = metadataStore.getConstraintCollectionByConstraintTypeAndScope(typeClass, scope).toSeq
    val resolvedConstraintCollections = conflictResolutionStrategy.resolve[Type](constraintCollections)
    require(resolvedConstraintCollections.size == 1, s"Found ${resolvedConstraintCollections.size} constraint collections.")
    resolvedConstraintCollections.head
  }

  /**
    * Retrieves all [[ConstraintCollection]]s and resolves potential conflicts if multiple ones are found.
    *
    * @param scope                      of the [[ConstraintCollection]]s
    * @param conflictResolutionStrategy decides how to act in the presence of multiple matching [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return the matching, resolved [[ConstraintCollection]]s
    */
  def findConstraintCollections[Type: ClassTag]
  (scope: Target, conflictResolutionStrategy: ConstraintCollectionConflictResolutionStrategy = FailOnConflictStrategy):
  Seq[ConstraintCollection[Type]] = {
    val typeClass = classTag[Type].runtimeClass.asInstanceOf[java.lang.Class[Type]]
    val constraintCollections = metadataStore.getConstraintCollectionByConstraintTypeAndScope(typeClass, scope).toSeq
    conflictResolutionStrategy.resolve[Type](constraintCollections)
  }

  /**
    * Retrieves all [[ConstraintCollection]]s for the given [[Constraint]] type whose scope is included in the given scope.
    *
    * @param scope of the [[ConstraintCollection]]s
    * @tparam Type the type of the [[Constraint]]s in the [[ConstraintCollection]]s to match
    * @return the matching, resolved [[ConstraintCollection]]s
    */
  def listConstraintCollections[Type <: Constraint : ClassTag]
  (scope: Target):
  Seq[ConstraintCollection[Type]] = {
    val typeClass = classTag[Type].runtimeClass.asInstanceOf[java.lang.Class[Type]]
    metadataStore.getIncludedConstraintCollections(typeClass, scope).toSeq
  }

  /**
    * Provide the [[Constraint]]s of a [[ConstraintCollection]].
    *
    * @param constraintCollectionId the ID of the [[ConstraintCollection]]
    * @param planBuilder            required to set up the Rheem plan
    * @tparam Type [[Constraint]] type
    * @return the [[Constraint]]s of the [[ConstraintCollection]] as [[DataQuanta]]
    */
  def loadConstraints[Type: ClassTag](constraintCollectionId: Int)(implicit planBuilder: PlanBuilder): DataQuanta[Type] = {
    val constraintCollection = metadataStore.getConstraintCollection(constraintCollectionId).asInstanceOf[ConstraintCollection[Type]]
    require(
      constraintCollection != null,
      message = s"No constraint collection with ID $constraintCollectionId found."
    )
    val constraintCls = classTag[Type].runtimeClass
    require(
      constraintCollection.getConstraintClass == constraintCls,
      message = s"Constraint collection is of type ${constraintCollection.getConstraintClass}. Expected $constraintCls, though."
    )

    // TODO: Depending on the underlying storage, there might be more efficient ways to ship the constraints to Rheem.
    planBuilder.loadCollection(constraintCollection.getConstraints)
  }

  /**
    * Provide the [[Constraint]]s of a [[ConstraintCollection]].
    *
    * @param constraintCollectionId the user-defined ID of the [[ConstraintCollection]]
    * @param planBuilder            required to set up the Rheem plan
    * @tparam Type [[Constraint]] type
    * @return the [[Constraint]]s of the [[ConstraintCollection]] as [[DataQuanta]]
    */
  def loadConstraints[Type: ClassTag](constraintCollectionId: String)(implicit planBuilder: PlanBuilder): DataQuanta[Type] = {
    val constraintCollection = Option(metadataStore.getConstraintCollection(constraintCollectionId).asInstanceOf[ConstraintCollection[Type]])
    loadConstraints(constraintCollection.toSeq: _*)
  }

  /**
    * Load one or more [[ConstraintCollection]]s as [[DataQuanta]].
    *
    * @param constraintCollections that should be loaded
    * @param planBuilder           required to construct the [[DataQuanta]]
    * @tparam Type of the [[Constraint]]s in the [[ConstraintCollection]]s
    * @return the [[DataQuanta]]
    */
  def loadConstraints[Type: ClassTag](constraintCollections: ConstraintCollection[Type]*)(implicit planBuilder: PlanBuilder):
  DataQuanta[Type] =
    constraintCollections.size match {
      case 0 => throw new NoConstraintCollectionException("No constraints given.")
      case 1 =>
        val constraintCollection = constraintCollections.head
        val constraintCls = classTag[Type].runtimeClass
        require(
          constraintCollection.getConstraintClass == constraintCls,
          message = s"Constraint collection is of type ${constraintCollection.getConstraintClass}. Expected $constraintCls, though."
        )
        // TODO: Depending on the underlying storage, there might be more efficient ways to ship the constraints to Rheem.
        planBuilder.loadCollection(constraintCollection.getConstraints)
      case _ => constraintCollections
        .map(cc => this.loadConstraints[Type](cc))
        .reduce(_ union _)
    }

  /**
    * Load the ID and (canonical) name of all [[de.hpi.isg.mdms.model.targets.Column]]s under the given [[Target]].
    *
    * @param target the [[Target]] or `null` if all [[Column]]s should be loaded
    * @return [[DataQuanta]] of column IDs and names
    */
  def loadColumns(target: Target = null)(implicit planBuilder: PlanBuilder): DataQuanta[ColumnMock] = {
    var columns: Iterable[Column] = target match {
      case null => metadataStore.getSchemas.flatMap(_.getTables).flatMap(_.getColumns)
      case column: Column => Seq(column)
      case table: Table => table.getColumns
      case schema: Schema => schema.getTables.flatMap(_.getColumns)
    }
    val idsWithNames = columns.map(c => new ColumnMock(c))
    planBuilder.loadCollection(idsWithNames)
  }

  /**
    * Load the ID and (canonical) name of all [[de.hpi.isg.mdms.model.targets.Table]]s under the given [[Target]].
    *
    * @param target the [[Target]] or `null` if all [[Table]]s should be loaded
    * @return [[DataQuanta]] of table IDs and names
    */
  def loadTables(target: Target = null)(implicit planBuilder: PlanBuilder): DataQuanta[TableMock] = {
    var tables: Iterable[Table] = target match {
      case null => metadataStore.getSchemas.flatMap(_.getTables)
      case table: Table => Seq(table)
      case schema: Schema => schema.getTables
      case _ => throw new IllegalArgumentException("Could not identify tables.")
    }
    val idsWithNames = tables.map(t => new TableMock(t))
    planBuilder.loadCollection(idsWithNames)
  }

  /**
    * Load the ID and (canonical) name of all [[de.hpi.isg.mdms.model.targets.Schema]]ta.
    *
    * @return [[DataQuanta]] of schema IDs and names
    */
  def loadSchemata()(implicit planBuilder: PlanBuilder): DataQuanta[SchemaMock] = {
    val idsWithNames = metadataStore.getSchemas.map(s => new SchemaMock(s))
    planBuilder.loadCollection(idsWithNames)
  }

}
