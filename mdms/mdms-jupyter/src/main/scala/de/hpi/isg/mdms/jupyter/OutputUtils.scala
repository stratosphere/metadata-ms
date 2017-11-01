package de.hpi.isg.mdms.jupyter

import java.util.Objects

import jupyter.api.Publish

import scala.collection.{GenSeq, GenTraversableOnce}

/**
  * This class provides utilities for nicely formatted output to Jupyter.
  */
class OutputUtils(publish: Publish) {

  /**
    * Output a list of bullet points.
    *
    * @param elements the elements to be listed
    * @param format   optional formatting method for each element
    * @param before   optional introductory text
    * @param after    optional concluding text
    * @tparam T the type of elements
    */
  def list[T](elements: GenTraversableOnce[T],
              format: T => String = (t: T) => t.toString,
              before: Option[String] = None,
              after: Option[String] = None): Unit = {
    val sb = new StringBuilder
    before.foreach { text => sb.append(s"<p>$text</p>") }
    sb.append("<ul>")
    elements.foreach { element => sb.append(s"<li>${format(element)}</li>") }
    sb.append("</ul>")
    after.foreach { text => sb.append(s"<p>$text</p>") }

    publish.html(sb.toString)
  }

  /**
    * Output a numbered list .
    *
    * @param elements the elements to be listed
    * @param format   optional formatting method for each element
    * @param before   optional introductory text
    * @param after    optional concluding text
    * @tparam T the type of elements
    */
  def enumeration[T](elements: GenTraversableOnce[T],
                     format: T => String = (t: T) => t.toString,
                     before: Option[String] = None,
                     after: Option[String] = None): Unit = {
    val sb = new StringBuilder
    before.foreach { text => sb.append(s"<p>$text</p>") }
    sb.append("<ol>")
    elements.foreach { element => sb.append(s"<li>${format(element)}</li>") }
    sb.append("</ol>")
    after.foreach { text => sb.append(s"<p>$text</p>") }

    publish.html(sb.toString)
  }


  /**
    * Output a table.
    *
    * @param elements the elements to be listed (= rows)
    * @param columns  pairs of (1) column names and (2) property extractor functions for the `elements`
    * @param before   optional introductory text
    * @param after    optional concluding text
    * @tparam T the type of elements
    */
  def table[T](elements: GenTraversableOnce[T],
               columns: GenSeq[(String, T => Any)],
               before: Option[String] = None,
               after: Option[String] = None): Unit = {
    val sb = new StringBuilder
    before.foreach { text => sb.append(s"<p>$text</p>") }
    sb.append("<table>")
    // Build the table header.
    sb.append("<tr>")
    columns.foreach { case (name, _) => sb.append("<th>").append(name).append("</th>") }
    sb.append("</tr>")
    // Build the table rows.
    elements.foreach { element =>
      sb.append("<tr>")
      columns.foreach { case (_, prop) => sb.append("<td>").append(Objects.toString(prop(element))).append("</td>") }
      sb.append("</tr>")
    }
    sb.append("</table>")
    after.foreach { text => sb.append(s"<p>$text</p>") }

    publish.html(sb.toString)
  }

  /**
    * Print the Metacrate logo.
    */
  def showLogo(): Unit = {
    val logo = ResourceManager.getBinary("/metacrate/logo.png")

    publish.png(logo)
  }

}
