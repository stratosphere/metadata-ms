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
    * @param yaxisTitle an optional tile for the y-axis
    */
  def plotBarChart[T: ClassTag, V: ClassTag](data: Traversable[(T, V)],
                                             title: String = null,
                                             xaxisTitle: String = null,
                                             yaxisTitle: String = null): Unit = {
    val (x, y) = data.unzip
    plotBarChart(x, y, title, xaxisTitle, yaxisTitle)
  }

  /**
    * Plots a plotly bar chart.
    *
    * @param x          the x values
    * @param y          the y values
    * @param title      an optional title for the chart
    * @param xaxisTitle an optional title for the x-axis
    * @param yaxisTitle an optional tile for the y-axis
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
    * @param yaxisTitle an optional tile for the y-axis
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

}

protected[jupyter] object Visualizations {

  private var isPlotlyScalaInitialized = false

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