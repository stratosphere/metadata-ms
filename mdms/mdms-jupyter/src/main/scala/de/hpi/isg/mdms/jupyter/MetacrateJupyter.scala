package de.hpi.isg.mdms.jupyter

import java.util.concurrent.atomic.AtomicInteger

import jupyter.api.Publish

/**
  * This object provides entry points into Metacrate's Jupyter utilities.
  */
object MetacrateJupyter {

  private val idCounter = new AtomicInteger

  /**
    * Initializes the Metacrate Jupyter utilities.
    */
  def init()(implicit publish: Publish): Unit = {
    // Enable D3 via requirejs.
    publish.js(
      """require.config({
        |  paths: {
        |    d3: 'https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min'
        |  }
        |});
      """.stripMargin)
  }

  /**
    * Adds a new `svg` element to the notebook with a new ID.
    *
    * @return the ID of the new element
    */
  private[jupyter] def addSvg(width: String = "100%",
                              height: String = "100%")
                             (implicit publish: Publish): String = {
    val id = s"metacrate-svg-${idCounter.getAndAdd(1)}"
    publish.html(s"""<svg id="$id" style="width: $width; height: $height"></svg>""")
    id
  }

  private[jupyter] def nextId() = idCounter.getAndAdd(1)

}
