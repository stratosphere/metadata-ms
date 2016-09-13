package de.hpi.isg.mdms.analytics.visualization

import java.io.StringWriter

import de.hpi.isg.mdms.analytics.GroupedConstraintCollection
import de.hpi.isg.mdms.model.constraints.Constraint
import io.continuum.bokeh._

import scala.math.floor
import scala.reflect.runtime.universe.TypeTag
import scala.xml.XML

/**
  * Handles all chart related functionality. Needs to be initialized with a RenderingConfig before usage.
  * Currently, bokeh-scala is used. It seems overly complicated for this simple use case, however,
  * there are not many suitable alternatives.
  */
object Chart {

  private var renderingConf: RenderingConf = UninitializedRenderingConfig
  val colorGen = new scala.util.Random
  val colors = Color.values.toArray

  /**
    * Initializes the Chart object.
    * @param renderConf RenderingConfig object to initialize with
    */
  def initialize(renderConf: RenderingConf): Unit = {
    renderingConf = renderConf
  }


  /**
    * Creates a histogram with String Keys and Double values.
    * @param plotData Map of string keys and double values
    * @return Plot object to be rendered
    */
  def createHistogram(plotData: Map[String, Double]): Plot = {
    val data = new DataSource(plotData)
    val xdr = new FactorRange().factors(data.x)
    val ydr = new DataRange1d()
    val bar = new Rect().x(data.xPositions).y('yOffset).fill_color(Color.Red).width(1).height('y)
    val renderer = new GlyphRenderer().data_source(data).glyph(bar)

    getPlot(xdr, ydr, List(renderer))
  }

  /**
    * Creates a line graph with String Keys and Double values.
    * @param plotData Map of string keys and double values
    * @return Plot object to be rendered
    */
  def createLineGraph(plotData: Map[String, Double]): Plot = {
    val data = new DataSource(plotData)
    val xdr = new FactorRange().factors(data.x)
    val ydr = new DataRange1d()
    val line = new Line().x(data.xPositions).y(data.y).line_color(Color.Blue)
    val renderer = new GlyphRenderer().data_source(data).glyph(line)

    getPlot(xdr, ydr, List(renderer))
  }

  /**
    * Creates a line graph with String Keys and Double values.
    * @param plotData Arbitrary sequences of (x, y, radius) tuples. Each sequence represents a unique color group
    * @return Plot object to be rendered
    */
  def createScatterPlot(plotData: Seq[(Double, Double, Double)]*): Plot = {
    val groupedData = plotData.flatMap { group =>
      val color = generateRandomColor()
      group.map { case (x, y, r) =>
        ScatterPoint(x, y, r, color)
      }
    }

    val xdr = new DataRange1d()
    val ydr = new DataRange1d()

    val glyphRenderers = buildScatterGlyphs(groupedData)
    getPlot(xdr, ydr, glyphRenderers, classOf[LinearAxis])
  }


  /**
    * Helper function to create a histogram from a GroupedJoin object.
    * @param grouped The GroupedJoin object to render
    * @return Plot object to render
    */
  def histogramFromGroupedJoin[A <: Constraint: TypeTag, B <: Constraint: TypeTag, K <: Any](grouped: GroupedConstraintCollection[A, B, K]): Plot = {
    val data = grouped.sizePerKey().map { case (key, size) => (key, size.toDouble)}
    histogramFromTuples(data)
  }

  /**
    * Helper function to create a histogram from a tuples.
    * @param tuples The key-value tuples to render
    * @return Plot object to render
    */
  def histogramFromTuples(tuples: Iterable[(Any, Double)]): Plot = {
    val data = tuples.map { case (a, b) => (a.toString, b) }.toMap
    createHistogram(data)
  }

  /**
   * https://github.com/bokeh/bokeh-scala/issues/24
   * Workaround until version 0.8
   * Call from jupyter with display.html(getBokehPlot(plot))
    *
    * Creates the renderable Javascript object to display in HTML.
    * @return Displayable plot object
  */
  def getBokehPlot(plot: Plot): String = {
    renderingConf.checkInitialization()
    val resources = Resources.default
    val fragment = new HTMLFragment(scala.xml.NodeSeq.Empty, resources.styles, resources.scripts)
    val document = new Document(plot)
    val writer = new StringWriter()
    XML.write(writer, <div> { fragment.preamble } </div>, "UTF-8", xmlDecl=false, doctype=null)
    XML.write(writer, document.fragment.html.head, "UTF-8", xmlDecl=false, doctype=null)

    writer.toString
  }

  private def getPlot(xRange: Range, yRange: Range,
                      renderers: Seq[Renderer], xAxisType: Class[_ <: Axis] = classOf[CategoricalAxis]): Plot = {

    val plot = new Plot().x_range(xRange).y_range(yRange)
    val xAxis = xAxisType.newInstance.plot(plot).location(Location.Below)
    val yAxis = new LinearAxis().plot(plot).location(Location.Left)
    plot.below <<= (xAxis :: _)
    plot.left <<= (yAxis :: _)
    plot.renderers := List(xAxis, yAxis) ++ renderers

    plot
  }

  private def generateRandomColor(): Color = {
    val index = colorGen.nextInt(colors.length)
    colors(index)
  }

  def buildScatterGlyphs(points: Seq[ScatterPoint]): Seq[GlyphRenderer] = {
    val source = new ColumnDataSource()
      .addColumn('x, points.map(_.x))
      .addColumn('y, points.map(_.y))

    points.map { point =>
      val circle = new Circle().x(point.x).y(point.y).radius(point.r, SpatialUnits.Data).fill_color(point.color)
      new GlyphRenderer().data_source(source).glyph(circle)
    }
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

private case class ScatterPoint(x: Double, y: Double, r: Double, color: Color)
