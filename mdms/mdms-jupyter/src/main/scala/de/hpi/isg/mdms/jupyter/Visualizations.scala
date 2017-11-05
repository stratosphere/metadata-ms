package de.hpi.isg.mdms.jupyter

import de.hpi.isg.mdms.jupyter.json.JsonSerializer._
import de.hpi.isg.mdms.jupyter.json.{JsonSerializable, JsonSerializer}
import jupyter.api.Publish
import plotly.element.ScatterMode
import plotly.layout.{Axis, Layout}
import plotly.{Bar, Scatter, Sequence}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * This class is meant to provide easy access to visualizations from within Jupyter notebooks.
  */
class Visualizations(publish: Publish) {

  import Visualizations._

  /**
    * Plots a plotly bar chart.
    *
    * @param data       `(x, y)` tuples to plot
    * @param title      an optional title for the chart
    * @param xaxisTitle an optional title for the x-axis
    * @param yaxisTitle an optional title for the y-axis
    * @param ordering   an optional plotting ordering
    */
  def plotBarChart[T: ClassTag, V: ClassTag](data: Traversable[(T, V)],
                                             title: String = null,
                                             xaxisTitle: String = null,
                                             yaxisTitle: String = null,
                                             ordering: Ordering[(T, V)] = null): Unit = {
    var orderedData = ordering match {
      case null => data
      case someOrdering => data.toSeq.sorted(someOrdering)
    }
    val (x, y) = orderedData.unzip
    plotBarChart(x, y, title, xaxisTitle, yaxisTitle)
  }

  /**
    * Plots a plotly bar chart.
    *
    * @param x          the x values
    * @param y          the y values
    * @param title      an optional title for the chart
    * @param xaxisTitle an optional title for the x-axis
    * @param yaxisTitle an optional title for the y-axis
    */
  def plotBarChart(x: Sequence,
                   y: Sequence,
                   title: String,
                   xaxisTitle: String,
                   yaxisTitle: String): Unit = {
    val layout = Layout(
      title = title,
      xaxis = if (xaxisTitle != null) Axis(title = xaxisTitle) else null,
      yaxis = if (yaxisTitle != null) Axis(title = yaxisTitle) else null
    )
    val trace = Bar(
      x = x, y = y
    )
    Visualizations.ensurePlotlyScalaInitialized(publish)
    plotly.JupyterScala.plot(data = Seq(trace), layout = layout)(publish)
  }

  /**
    * Plots a plotly scatter chart.
    *
    * @param x          the x values
    * @param y          the y values
    * @param title      an optional title for the chart
    * @param xaxisTitle an optional title for the x-axis
    * @param yaxisTitle an optional title for the y-axis
    */
  def plotScatterChart(x: Sequence,
                       y: Sequence,
                       text: Seq[String],
                       title: String,
                       xaxisTitle: String,
                       yaxisTitle: String,
                       mode: ScatterMode): Unit = {
    val layout = Layout(
      title = title,
      xaxis = if (xaxisTitle != null) Axis(title = xaxisTitle) else null,
      yaxis = if (yaxisTitle != null) Axis(title = yaxisTitle) else null
    )
    val trace = Scatter(
      values = x,
      secondValues = y,
      text = text,
      mode = mode
    )
    Visualizations.ensurePlotlyScalaInitialized(publish)
    plotly.JupyterScala.plot(data = Seq(trace), layout = layout)(publish)
  }

  /**
    * Plot a directed graph.
    *
    * @param nodes        the vertices of the graph as `(label, size)` tuples
    * @param links        the edges of the graph as `(label, label)` tuples
    * @param width        optional width of the graph
    * @param height       optional height of the graph
    * @param linkDistance optional link distance setting
    * @param charge       optional node charge setting (usually negative)
    * @param publish      Jupyter-scala capability
    */
  def plotDirectedGraph(links: Iterable[GraphEdge],
                        nodes: Iterable[GraphVertex] = null,
                        width: String = "100%",
                        height: String = "100%",
                        linkDistance: Int = 100,
                        charge: Int = -300)
                       (implicit publish: Publish) = {

    ensureD3Initialized(publish)

    // Initialize the canvas.
    val svgId = addSvg(height = height)

    // Build the nodes array.
    val jsNodes = JsonSerializer.toJson({
      if (nodes == null) links.flatMap(edge => Seq(edge.destination, edge.source)).map(name => GraphVertex(name)).toSet
      else nodes
    })

    // Build the links array.
    val jsLinks = JsonSerializer.toJson(links)

    val frontendVariables = Map(
      "svgId" -> svgId,
      "nodes" -> jsNodes,
      "links" -> jsLinks,
      "linkDistance" -> linkDistance.toString,
      "charge" -> charge.toString,
      "pathStyle" -> s"$svgId-path",
      "circleStyle" -> s"$svgId-circle",
      "textStyle" -> s"$svgId-text"
    )

    // Initialize the styles.
    val styleHtml = ResourceManager.get("/metacrate/directed-graph.html", frontendVariables)
    publish.html(styleHtml)

    // Build the JS script.
    val js = ResourceManager.get("/metacrate/directed-graph.js", frontendVariables)
    publish.js(js)
  }

  /**
    * Plot a grid.
    *
    * @param tiles   that make up the grid
    * @param publish to plot
    */
  def plotGrid(tiles: Iterable[Tile],
               width: Int = 500,
               height: Int = 500,
               marginLeft: Int = 200,
               marginTop: Int = 200,
               rows: Iterable[Category] = null,
               columns: Iterable[Category] = null)
              (implicit publish: Publish) = {

    // Convert the input data to JSON.
    val jsTiles = JsonSerializer.toJson(tiles)
    val jsRows = JsonSerializer.toJson(if (rows == null) tiles.map(tile => Category(tile.row)).toSet else rows)
    val jsColumns = JsonSerializer.toJson(if (columns == null) tiles.map(tile => Category(tile.column)).toSet else columns)

    // Publish the style sheet.
    val html = ResourceManager.get("/metacrate/tile-matrix.html")
    publish.html(html)

    // Create the SVG element.
    val svgId = addSvg()

    // Publish the script with the data.
    val js = ResourceManager.get("/metacrate/tile-matrix.js", Map(
      "tiles" -> jsTiles,
      "rows" -> jsRows,
      "columns" -> jsColumns,
      "svgId" -> svgId,
      "width" -> width.toString,
      "height" -> height.toString,
      "marginLeft" -> marginLeft.toString,
      "marginTop" -> marginTop.toString
    ))
    publish.js(js)
  }

  /**
    * Plot a chord diagram.
    *
    * @param transitions   that make up the chords
    * @param groups        optional groups to consider (with optional weight)
    * @param scaleByGroups whether to scale the sections by the `groups` weights
    * @param diameter      the diameter of the chord diagram
    * @param publish       to plot
    */
  def plotChordDiagram(transitions: Iterable[Transition],
                       groups: Iterable[Category] = null,
                       scaleByGroups: Boolean = false,
                       diameter: Int = 700)
                      (implicit publish: Publish) = {

    // Convert the input data to JSON.
    val jsTransitions = JsonSerializer.toJson(transitions)
    val jsGroups = JsonSerializer.toJson(
      if (groups != null) groups
      else transitions.flatMap(t => Seq(t.source, t.destination)).toSet.map((name: String) => Category(name = name))
    )

    // Publish the style sheet.
    val html = ResourceManager.get("/metacrate/chord-diagram.html")
    publish.html(html)

    // Create the SVG element.
    val svgId = addSvg()

    // Publish the script with the data.
    val js = ResourceManager.get("/metacrate/chord-diagram.js", Map(
      "groups" -> jsGroups,
      "scaleByGroups" -> scaleByGroups.toString,
      "transitions" -> jsTransitions,
      "diameter" -> diameter.toString,
      "svgId" -> svgId
    ))
    publish.js(js)
  }

  /**
    * Plot a sunburst chart.
    *
    * @param paths             that make up the sections of the sunburst
    * @param width             of the sunburn chart
    * @param height            of the sunburn chart
    * @param showLegend        whether to a legend should be displayed
    * @param reverseBreadcrumb whether the breadcrumb should be displayed in reverse order
    * @param publish           the Jupyter adapter
    *
    */
  def plotSunburst(paths: Iterable[Path],
                   width: Int = 700,
                   height: Int = 600,
                   showLegend: Boolean = false,
                   reverseBreadcrumb: Boolean = false)
                  (implicit publish: Publish): Unit = {

    val jsVariables = Map(
      "id" -> nextId().toString,
      "width" -> width.toString,
      "height" -> height.toString,
      "paths" -> JsonSerializer.toJson(paths),
      "showLegend" -> showLegend.toString,
      "reverseLegend" -> reverseBreadcrumb.toString
    )

    // Publish the style sheet.
    val html = ResourceManager.get("/metacrate/sunburst.html", jsVariables)
    publish.html(html)

    val js = ResourceManager.get("/metacrate/sunburst.js", jsVariables)
    publish.js(js)
  }

}

protected[jupyter] object Visualizations {

  private var isPlotlyScalaInitialized = false

  private var isD3Initialized = false

  /**
    * Makes sure that plotly-scala is initialized. If it already is, nothing happens.
    *
    * @param publish jupyter-scala capability
    */
  private def ensurePlotlyScalaInitialized(publish: Publish): Unit =
    if (!isPlotlyScalaInitialized) {
      plotly.JupyterScala.init(offline)(publish)
      isPlotlyScalaInitialized = true
    }

  /**
    * Makes sure that d3 is initialized. If it already is, nothing happens.
    *
    * @param publish jupyter-scala capability
    */
  private def ensureD3Initialized(publish: Publish): Unit =
    if (!isD3Initialized) {
      val js = if (offline) ResourceManager.get("/d3.min.js") else ResourceManager.get("/metacrate/init-d3.js")
      publish.js(js)
      isD3Initialized = true
    }


  /**
    * Transform a [[TraversableOnce]] into a [[Sequence]] by looking at the included data type.
    *
    * @param iterable that should be converted
    * @tparam T the element type of `iterable`
    * @return the [[Sequence]]
    */
  protected[jupyter] implicit def asSequence[T: ClassTag](iterable: TraversableOnce[T]): Sequence = {
    val t = scala.reflect.classTag[T].runtimeClass
    if (t == classOf[Int]) Sequence.fromIntSeq(iterable.toSeq.asInstanceOf[Seq[Int]])
    else if (t == classOf[Long]) Sequence.fromLongSeq(iterable.toSeq.asInstanceOf[Seq[Long]])
    else if (t == classOf[Double]) Sequence.fromDoubleSeq(iterable.toSeq.asInstanceOf[Seq[Double]])
    else if (t == classOf[Float]) Sequence.fromFloatSeq(iterable.toSeq.asInstanceOf[Seq[Float]])
    else Sequence.fromStringSeq(iterable.map(v => if (v == null) null else v.toString).toSeq)
  }

}

case class GraphEdge(source: String, destination: String) extends JsonSerializable {

  import JsonSerializer.{escape => esc}

  override def toJson: String = s"""{source:"${esc(source)}",target:"${esc(destination)}"}"""
}

case class GraphVertex(name: String, size: Int = 5, color: Int = 0) extends JsonSerializable {

  import JsonSerializer.{escape => esc}

  override def toJson: String = s"""{name:"${esc(name)}",size:$size,color:$color}"""
}

case class Tile(row: String, column: String, opacity: Double = 1d, color: Int = 0) extends JsonSerializable {

  import JsonSerializer.{escape => esc}

  override def toJson: String = s"""{row:"${esc(row)}",column:"${esc(column)}",opacity:$opacity,color:$color}"""
}

case class Category(name: String, order: Int = 0, weight: Double = 1d) extends JsonSerializable {

  import JsonSerializer.{escape => esc}

  override def toJson: String = s"""{name:"${esc(name)}",order:$order,weight:$weight}"""
}

case class Transition(source: String, destination: String, value: Double = 1d) extends JsonSerializable {

  import JsonSerializer.{escape => esc}

  override def toJson: String = s"""{source:"${esc(source)}",destination:"${esc(destination)}",value:$value}"""
}

case class Path(elements: Seq[String]) extends JsonSerializable {

  override def toJson: String = s"""${JsonSerializer.toJson(elements)(stringToJson)}"""

}