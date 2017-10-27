package de.hpi.isg.mdms.jupyter

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
  def plotDirectedGraph(nodes: Iterable[(String, Int)],
                        links: Iterable[(String, String)],
                        width: String = "100%",
                        height: String = "100%",
                        linkDistance: Int = 100,
                        charge: Int = -300)
                       (implicit publish: Publish) = {

    ensureD3Initialized(publish)

    // Initialize the canvas.
    val svgId = addSvg(height = height)

    // Build the nodes array.
    val jsNodes = {
      val jsonNodes = nodes.map { case (name, size) => s"""{name:"$name",size:$size}""" }
      s"[${jsonNodes.mkString(",")}]"
    }

    // Build the links array.
    val jsLinks = {
      val jsonLinks = links.map { case (src, dest) => s"""{source:"$src",target:"$dest"}""" }
      s"[${jsonLinks.mkString(",")}]"
    }


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
    * Plot a tile matrix.
    *
    * @param nodes   the domain for the axes: `(name, id)`
    * @param values  the coordinate values: `(source id, target id, distance)`
    * @param publish to plot
    */
  def plotHeatMap(nodes: Iterable[(String, Int)],
                  values: Iterable[(Int, Int, Double)],
                  width: Int)
                 (implicit publish: Publish) = {

    // Convert the input data to JSON.
    val jsNodes = nodes.map {
      case (name, id) => s"""{"name":"$name", "id":$id}"""
    }.mkString("[", ",", "]")

    val jsDistances = values.map {
      case (source, target, value) => s"""{"source":$source, "target":$target, "value":$value}"""
    }.mkString("[", ",", "]")

    // Publish the style sheet.
    val html = ResourceManager.get("/metacrate/tile-matrix.html")
    publish.html(html)

    // Create the SVG element.
    val svgId = addSvg()


    // Publish the script with the data.
    val js = ResourceManager.get("/metacrate/tile-matrix.js", Map(
      "nodes" -> jsNodes,
      "links" -> jsDistances,
      "svgId" -> svgId,
      "width" -> width.toString
    ))
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
      val js = ResourceManager.get("/metacrate/init-d3.js")
      publish.js(js)
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