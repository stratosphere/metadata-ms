package de.hpi.isg.mdms.analytics

import java.io.StringWriter

import io.continuum.bokeh._

import scala.math.floor


object Visualization {

  def createHistogram(plotData: Map[String, Double]): Plot = {
    val data = new DataSource(plotData)
    val xdr = new FactorRange().factors(data.x)
    val ydr = new DataRange1d()
    val bar = new Rect().x(data.xPositions).y('yOffset).fill_color(Color.Red).width(1).height('y)
    val renderer = new GlyphRenderer().data_source(data).glyph(bar)

    getPlot(xdr, ydr, renderer)
  }

  def createLineGraph(plotData: Map[String, Double]): Plot = {
    val data = new DataSource(plotData)
    val xdr = new FactorRange().factors(data.x)
    val ydr = new DataRange1d()
    val line = new Line().x(data.xPositions).y(data.y).line_color(Color.Blue)
    val renderer = new GlyphRenderer().data_source(data).glyph(line)

    getPlot(xdr, ydr, renderer)
  }


  /*
   * https://github.com/bokeh/bokeh-scala/issues/24
   * Workaround until version 0.8
   * Call from jupyter with display.html(initBokeh)
  */
  def initBokeh: String = {
    val resources = Resources.default
    val fragment = new HTMLFragment(scala.xml.NodeSeq.Empty, resources.styles, resources.scripts)
    val writer = new java.io.StringWriter()
    scala.xml.XML.write(writer, <div> {  fragment.preamble } </div>, "UTF-8", xmlDecl=false, doctype=null)

    writer.toString
  }

  /*
   * https://github.com/bokeh/bokeh-scala/issues/24
   * Workaround until version 0.8
   * Call from jupyter with display.html(getBokehPlot(plot))
  */
  def getBokehPlot(plot: Plot): String = {
    val writer = new StringWriter()
    val document = new Document(plot)
    scala.xml.XML.write(writer, document.fragment.html(0), "UTF-8", xmlDecl=false, doctype=null)

    writer.toString
  }


  private def getPlot(xRange: Range, yRange: Range,
                      renderer: Renderer, xAxisType: Class[_ <: Axis] = classOf[CategoricalAxis]): Plot = {

    val plot = new Plot().x_range(xRange).y_range(yRange)
    val xAxis = xAxisType.newInstance.plot(plot).location(Location.Below)
    val yAxis = new LinearAxis().plot(plot).location(Location.Left)
    plot.below <<= (xAxis :: _)
    plot.left <<= (yAxis :: _)
    plot.renderers := List(xAxis, yAxis, renderer)

    plot
  }
}

private[analytics] class DataSource(plotData: Map[String, Double]) extends ColumnDataSource {
  val dataList = plotData.toList
  val x = dataList.map(_._1)
  val values = dataList.map(_._2)
  val y = column(values)

  val xPositions = column(1.0 to dataList.length by 1.0)
  val yOffset = column(values.map(value => floor(value / 2)))
}
