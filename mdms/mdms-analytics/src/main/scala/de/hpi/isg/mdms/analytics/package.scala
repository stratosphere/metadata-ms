package de.hpi.isg.mdms

import de.hpi.isg.mdms.analytics.rheem.{JoinedDataQuanta, MetadataQuanta, MetadataStoreRheemWrapper}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.targets.{Column, Schema, Table, Target}
import org.qcri.rheem.api.DataQuanta

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
  * Provides utilities to analyze metadata store contents.
  */
package object analytics {

  /**
    * Expose Rheem capabilities on a [[MetadataStore]].
    *
    * @param metadataStore the [[MetadataStore]]
    * @return a [[MetadataStoreRheemWrapper]] exposing Rheem capabilities
    */
  implicit def pimpMetadataStore(metadataStore: MetadataStore): MetadataStoreRheemWrapper =
    new MetadataStoreRheemWrapper(metadataStore)

  implicit def pimpDataQuanta[Out: ClassTag](dataQuanta: DataQuanta[Out]): MetadataQuanta[Out] =
    new MetadataQuanta(dataQuanta)

  implicit def pimpJoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]
  (dataQuanta: DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out0, Out1]]): JoinedDataQuanta[Out0, Out1] =
    new JoinedDataQuanta(dataQuanta)

  /**
    * Collects all [[Column]]s contained by the given `targets`.
    * @param targets any kind of [[Target]]s or `null`
    * @return a [[Set]] of all contained [[Column]]s.
    */
  def collectColumns(targets: Target*): Set[Column] =
    targets.flatMap {
      case column: Column => Seq(column)
      case table: Table => collectColumns(table.getColumns.toSeq: _*)
      case schema: Schema => collectColumns(schema.getTables.toSeq: _*)
      case null => Seq()
    }.toSet

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always fails on conflicts.
    */
  object FailOnConflictStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) throw new ConstraintCollectionConflictException(conflictDomain)
      conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always takes the first element from the conflict domain.
    */
  object TakeFirstStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) Seq(conflictDomain.head)
      else conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that always takes the first element from the conflict domain.
    */
  object TakeLastStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] = {
      if (conflictDomain.size > 1) Seq(conflictDomain.last)
      else conflictDomain
    }
  }

  /**
    * [[ConstraintCollectionConflictResolutionStrategy]] that resolves conflict by taking all [[ConstraintCollection]]s.
    */
  object TakeAllStrategy extends ConstraintCollectionConflictResolutionStrategy {
    override def resolve[T](conflictDomain: Seq[ConstraintCollection[T]]): Seq[ConstraintCollection[T]] =
      conflictDomain
  }

  /**
    * Light-weight representation for a [[Target]].
    *
    * @param id   the [[Target]] ID
    * @param name the [[Target]] name
    */

  case class TargetMock(id: Int, name: Int)

  /**
    * Light-weight representation for a [[Schema]].
    *
    * @param id   the [[Schema]] ID
    * @param name the [[Schema]] name
    */
  case class SchemaMock(id: Int, name: String) {
    def this(schema: Schema) = this(schema.getId, schema.getName)
  }

  /**
    * Light-weight representation for a [[Table]].
    *
    * @param id     the [[Table]] ID
    * @param name   the [[Table]] name
    * @param schema the name of the parent [[Schema]]
    */
  case class TableMock(id: Int, name: String, schema: String) {
    def this(table: Table) = this(table.getId, table.getName, table.getSchema.getName)

    def nameWithSchema = schema + "." + name

  }

  /**
    * Light-weight representation for a [[Column]].
    *
    * @param id     the [[Column]] ID
    * @param name   the [[Column]] name
    * @param schema the name of the parent [[Schema]]
    * @param table  the name of the parent [[Table]]
    */
  case class ColumnMock(id: Int, name: String, schema: String, table: String) {
    def this(column: Column) = this(column.getId, column.getName, column.getTable.getSchema.getName, column.getTable.getName)

    def nameWithTable = table + "." + name

    def nameWithSchema = schema + "." + table + "." + name
  }

}
