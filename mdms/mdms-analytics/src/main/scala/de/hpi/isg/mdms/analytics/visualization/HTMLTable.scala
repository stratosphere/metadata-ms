package de.hpi.isg.mdms.analytics.visualization

import de.hpi.isg.mdms.analytics.ConstraintImplicits.ConstraintCollectionQueryObject
import de.hpi.isg.mdms.analytics.{GroupedConstraintCollection, JoinedConstraintCollection}
import de.hpi.isg.mdms.domain.constraints.{InclusionDependency, UniqueColumnCombination}
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.xml.Elem

object HTMLTable {

  private var renderingConf: RenderingConf = UninitializedRenderingConfig

  val baseTable = <table style="border: 1px solid black; border-collapse: collapse;"/>
  val emptyTable = createEmptyTable()

  /**
    * Initializes the HTMLTable object.
    * @param renderingConfig RenderingConfig object to initialize with
    */
  def initialize(renderingConfig: RenderingConf): Unit = {
    renderingConf = renderingConfig
  }

  /**
    * Returns a table with two columns, one for each tuple side.
    * @param joined The JoinedConstraintCollection to be displayed.
    */
  def fromJoin(joined: JoinedConstraintCollection[_ <: Constraint, _ <: Constraint]): Elem = {
    fromCollectionTuple(joined.selectAll())
  }

  /**
    * Returns a table with three columns, one for the key and one for each tuple side.
    * @param grouped The GroupedConstraintCollection to be displayed.
    */
  def fromGroupedJoin[A <: Constraint: TypeTag, B <: Constraint: TypeTag, K](grouped: GroupedConstraintCollection[A, B, K]): Elem = {
    val data = grouped.selectAll().flatMap { case (k, group) =>
      group.map { case (a, b) => List(k, a, b) }
    }

    val head = List("Key", genericConstraintName[A], genericConstraintName[B])
    newTable(data, Some(head))
  }

  /**
    * Returns a table with all elements in a ConstraintCollection.
    */
  def fromCollection[T<:Constraint](collection: ConstraintCollection[T]): Elem = {
    fromCollection(collection.constraintsIter)
  }

  /**
    * Returns a table with all elements in a Constraint Iterable.
    */
  def fromCollection[A <: Constraint: TypeTag](collection: Iterable[A]): Elem = {
    val data = collection.map(List(_))
    val head = List(genericConstraintName[A])
    newTable(data, Some(head))
  }

  /**
    * Returns a table with all tuples in a Constraint Iterable.
    */
  def fromCollectionTuple[A <: Constraint: TypeTag, B <: Constraint: TypeTag](collection: Iterable[(A, B)]): Elem = {
    val head = List(genericConstraintName[A], genericConstraintName[B])
    fromTuples(collection, Some(head))
  }

  /**
    * Returns a table with a header and two columns, one for each side.
    * @param tuples The tuples to be displayed.
    * @param head  Header in the table.
    */
  def fromTuples(tuples: Iterable[(Any, Any)], head: Option[Iterable[Any]]): Elem = {
    val data = tuples.map { case (a, b) => List(a, b) }
    newTable(data, head)
  }

  /**
    * Creates a HTML table from list of lists, with an optional header.
    */
  def newTable(data: Iterable[Iterable[Any]], head: Option[Iterable[Any]] = None): Elem = {
    if (dataIsEmpty(data)) {
      return emptyTable
    }

    val table = if (head.isDefined) addRowToTable(baseTable, createTableHead(head.get)) else baseTable
    addRowsToTable(table, createTableRows(data))
  }

  /**
    * Creates a table with no elements contained.
    */
  private def createEmptyTable(): Elem = {
    addRowToTable(baseTable, <tr>No Elements in table</tr>)
  }

  /**
    * Builds the rows of the table from a list of lists.
    */
  private def createTableRows(rows: Iterable[Iterable[Any]]): Iterable[Elem] = {
    renderingConf.checkInitialization()
    val dpp = new DependencyPrettyPrinter(renderingConf.mds)
    rows.map(row =>
      <tr>
        {
        row.map(cell =>
          <td style="border: 1px solid black; text-align: left;"> {
            cell match {
              case ind: InclusionDependency => dpp.prettyPrint(ind)
              case ucc: UniqueColumnCombination => dpp.prettyPrint(ucc)
              case _ => cell
            }
            }
          </td>)
        }
      </tr>)
  }

  /**
    * Creates the header from a list of values.
    */
  private def createTableHead(headRow: Iterable[Any]): Elem = {
    <tr>
      {
      headRow.map(cell =>
        <th style="border: 1px solid black; text-align: center;">
          { cell }
        </th>)
      }
    </tr>
  }


  private def addRowToTable(table: Elem, row: Elem): Elem = {
    table.copy(child = table.child ++ row)
  }

  private def addRowsToTable(table: Elem, rows: Iterable[Elem]): Elem = {
    table.copy(child = table.child ++ rows)
  }

  private def dataIsEmpty(data: Iterable[Iterable[Any]]): Boolean = {
    data.isEmpty || data.head.isEmpty
  }

  /**
    * Gets the name of a given class.
    */
  private def genericConstraintName[A: TypeTag]: String = {
    typeOf[A].toString.split("\\.").last
  }

}
