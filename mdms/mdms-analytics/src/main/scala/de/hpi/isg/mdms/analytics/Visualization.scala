package de.hpi.isg.mdms.analytics

import io.continuum.bokeh._

import scala.math.floor


object Visualization {

  def createHistogram(plotData: Map[String, Double]): Plot = {
    val data = new HistogramSource(plotData)
    val xdr = new FactorRange().factors(data.x)
    val ydr = new DataRange1d()
    val bar = new Rect().x(data.xPositions).y('yOffset).fill_color(Color.Red).width(1).height('y)
    val renderer = new GlyphRenderer().data_source(data).glyph(bar)

    getPlot(xdr, ydr, renderer)
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

private[analytics] class HistogramSource(plotData: Map[String, Double]) extends ColumnDataSource {
  val dataList = plotData.toList
  val x = dataList.map(_._1)
  val values = dataList.map(_._2)
  val y = column(values)

  val xPositions = column(1.0 to dataList.length by 1.0)
  val yOffset = column(values.map(value => floor(value / 2)))
}
