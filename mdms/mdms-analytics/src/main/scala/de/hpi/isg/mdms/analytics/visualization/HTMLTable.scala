package de.hpi.isg.mdms.analytics.visualization

import de.hpi.isg.mdms.analytics.ConstraintImplicits.ConstraintCollectionQueryObject
import de.hpi.isg.mdms.analytics.{GroupedConstraintCollection, JoinedConstraintCollection}
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}

import scala.xml.Elem

object HTMLTable {

  def fromJoin(joined: JoinedConstraintCollection[Constraint, Constraint]): Elem = {
    val data = joined.selectAll().map { case (a, b) => List(a, b) }.toList
    val head = List("Left", "Right")
    newTable(head :: data, containsHead = true)
  }

  def fromGroupedJoin(grouped: GroupedConstraintCollection[Constraint, Constraint, Any]): Elem = {
    val data = grouped.selectAll().flatMap { case (k, group) =>
      group.map { case (a, b) => List(k, a, b) }
    }.toList

    val head = List("Key", "Left", "Right").asInstanceOf[List[Any]]
    newTable(head :: data, containsHead = true)
  }

  def fromCollection(collection: ConstraintCollection): Elem = {
    fromCollection(collection.constraintsIter)
  }

  def fromCollection[A <: Constraint](collection: Iterable[A]): Elem = {
    val data = collection.map(List(_)).toList
    val head = collection.head.getClass.getSimpleName
    newTable(head :: data, containsHead = true)
  }

  def newTable(allData: Iterable[Iterable[Any]], containsHead: Boolean = false): Elem = {
    var data = allData
    var table = <table style="border: 1px solid black; border-collapse: collapse;"/>

    if (containsHead) {
      val headRow = allData.head
      data = allData.tail
      val tableHead = createTableHead(headRow)
      table = addRowToTable(table, tableHead)
    }

    val tableRows = createTableRows(data)
    table = addRowsToTable(table, tableRows)
    table
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
}
