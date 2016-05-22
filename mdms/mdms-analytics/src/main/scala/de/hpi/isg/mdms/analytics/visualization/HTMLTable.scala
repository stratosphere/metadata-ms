package de.hpi.isg.mdms.analytics.visualization

import de.hpi.isg.mdms.analytics.ConstraintImplicits.ConstraintCollectionQueryObject
import de.hpi.isg.mdms.analytics.{GroupedConstraintCollection, JoinedConstraintCollection}
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import scala.reflect.runtime.universe.{typeOf, TypeTag}

import scala.xml.Elem

object HTMLTable {

  // TODO: Handle empty lists
  val baseTable = <table style="border: 1px solid black; border-collapse: collapse;"/>
  val emptyTable = createEmptyTable()

  def fromJoin(joined: JoinedConstraintCollection[_ <: Constraint, _ <: Constraint]): Elem = {
    fromCollectionTuple(joined.selectAll())
  }

  def fromGroupedJoin[A <: Constraint: TypeTag, B <: Constraint: TypeTag, K <: Any](grouped: GroupedConstraintCollection[A, B, K]): Elem = {
    val data = grouped.selectAll().flatMap { case (k, group) =>
      group.map { case (a, b) => List(k, a, b) }
    }

    val head = List(s"Key", genericConstraintName[A], genericConstraintName[B])
    newTable(data, Some(head))
  }

  def fromCollection(collection: ConstraintCollection): Elem = {
    fromCollection(collection.constraintsIter)
  }

  def fromCollection[A <: Constraint: TypeTag](collection: Iterable[A]): Elem = {
    val data = collection.map(List(_))
    val head = List(genericConstraintName[A]) //collection.head.getClass.getSimpleName
    newTable(data, Some(head))
  }

  def fromCollectionTuple[A <: Constraint: TypeTag, B <: Constraint: TypeTag](collection: Iterable[(A, B)]): Elem = {
    val data = collection.map { case (a, b) => List(a, b) }
    val head = List(genericConstraintName[A], genericConstraintName[B])
    //List(collection.head._1.getClass.getSimpleName, collection.head._2.getClass.getSimpleName)
    newTable(data, Some(head))
  }

  def newTable(data: Iterable[Iterable[Any]], head: Option[Iterable[Any]] = None): Elem = {
    if (dataIsEmpty(data)) {
      return emptyTable
    }

    val table = if (head.isDefined) addRowToTable(baseTable, createTableHead(head.get)) else baseTable
    addRowsToTable(table, createTableRows(data))
  }

  private def createEmptyTable(): Elem = {
    addRowToTable(baseTable, <tr>No Elements in table</tr>)
  }

  private def createTableRows(rows: Iterable[Iterable[Any]]): Iterable[Elem] = {
    rows.map(row =>
      <tr>
        {
        row.map(cell =>
          <td style="border: 1px solid black; text-align: left;">
            { cell }
          </td>)
        }
      </tr>)
  }

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

  private def genericConstraintName[A: TypeTag]: String = {
    typeOf[A].toString.split("\\.").last
  }

}