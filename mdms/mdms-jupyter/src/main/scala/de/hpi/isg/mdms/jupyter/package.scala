package de.hpi.isg.mdms

import _root_.java.util.concurrent.atomic.AtomicInteger

import _root_.jupyter.api.Publish
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil
import de.hpi.isg.mdms.model.MetadataStore
import org.qcri.rheem.api.{DataQuanta, PlanBuilder}
import org.qcri.rheem.core.api.RheemContext
import plotly.element.ScatterMode

import scala.language.implicitConversions
import scala.languageFeature.implicitConversions
import scala.reflect.ClassTag

/**
  * This package object provides the Metacrate-speficif API for Jupyter.
  */
package object jupyter {

  /**
    * Tells whether the notebook should run offline.
    */
  var offline = true

  /**
    * Provides IDs for Jupyter HTML elements.
    */
  private var idCounter = new AtomicInteger

  /**
    * Provides an instance of [[OutputUtils]] for nicely formatted output.
    *
    * @param publish the Jupyter output adapter
    * @return the [[OutputUtils]]
    */
  def output(implicit publish: Publish) = new OutputUtils(publish)

  /**
    * Provides an instance of [[Visualizations]] for plotting, e.g., charts.
    *
    * @param publish the Jupyter output adapter
    * @return the [[Visualizations]]
    */
  def visualizations(implicit publish: Publish) = new Visualizations(publish)

  /**
    * Creates a new [[MetadataStore]].
    *
    * @param storageBackend in which the new instance should be created
    * @param isOverwrite    whether an existing instance should be overwritten
    * @param numTableBits   the number of bits in IDs that are used to identify tables
    * @param numColumnBits  the number of bits in IDs that are used to identify columns
    * @param publish        for integration with Jupyter
    * @return the new [[MetadataStore]]
    */
  def createMetacrate(storageBackend: StorageBackend,
                      isOverwrite: Boolean = false,
                      numTableBits: Int = 14,
                      numColumnBits: Int = 14)
                     (implicit publish: Publish): MetadataStore = {
    val params = new MetadataStoreParameters
    storageBackend match {
      case SqliteBackend(location) =>
        params.isDemandJavaSerialized = false
        params.metadataStore = location
      case JavaSerializedBackend(location) =>
        params.isDemandJavaSerialized = true
        params.metadataStore = location
      case _ => sys.error("Cannot handle storage backend")
    }
    val metacrate = MetadataStoreUtil.createMetadataStore(params, isOverwrite, numTableBits, numColumnBits)
    metacrate.flush()
    metacrate
  }


  /**
    * Loads an existing [[MetadataStore]].
    *
    * @param storageBackend in which the existing instance resides
    * @param publish        for integration with Jupyter
    * @return the loaded [[MetadataStore]]
    */
  def loadMetacrate(storageBackend: StorageBackend)(implicit publish: Publish): MetadataStore = {
    val params = new MetadataStoreParameters
    storageBackend match {
      case SqliteBackend(location) =>
        params.isDemandJavaSerialized = false
        params.metadataStore = location
      case JavaSerializedBackend(location) =>
        params.isDemandJavaSerialized = true
        params.metadataStore = location
      case _ => sys.error("Cannot handle storage backend")
    }
    MetadataStoreUtil.loadMetadataStore(params)
  }

  /**
    * This function makes queries in Jupyter notebooks look a bit nicer.
    *
    * @param f            the user-defined query
    * @param rheemContext that provides the execution facilities
    * @tparam T the return type of the query
    * @return the query result
    */
  def query[T](f: PlanBuilder => T)(implicit rheemContext: RheemContext): T = f(new PlanBuilder(rheemContext))

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

  /**
    * Provide the next ID for HTML elements.
    *
    * @return the ID
    */
  private[jupyter] def nextId() = idCounter.getAndAdd(1)

  /**
    * This class adds additional methods to [[DataQuanta]] of 2-tuples.
    *
    * @param dataQuanta the [[DataQuanta]]
    */
  implicit class GenericDataQuantaWrapper[T: ClassTag](dataQuanta: DataQuanta[T]) {

    /**
      * Plot a bar chart from the [[DataQuanta]]. The tuples are interpreted as `(x, y)` data points.
      *
      * @param format   optional formatter for the data points
      * @param after    optional text to display after the list
      * @param before   optional text to display before the list
      * @param ordering an optional plotting ordering
      * @param publish  adapter for Jupyter-Scala
      */
    def printList(format: T => String = (t: T) => t.toString,
                  before: String = null,
                  after: String = null,
                  ordering: Ordering[T] = null)
                 (implicit publish: Publish): Unit = {

      val data = ordering match {
        case null => dataQuanta.collect()
        case someOrdering => dataQuanta.collect().toSeq.sorted(someOrdering)
      }
      output.list(
        elements = data,
        before = Option(before),
        after = Option(after),
        format = format
      )
    }

  }

  /**
    * Convert [[DataQuanta]] to an [[Iterable]].
    *
    * @param dataQuanta the [[DataQuanta]]
    * @tparam T the type of the contained data quanta
    * @return the converted [[Iterable]]
    */
  implicit def collectDataQuanta[T](dataQuanta: DataQuanta[T]): Iterable[T] =
    if (dataQuanta == null) null else dataQuanta.collect()


  /**
    * This class adds additional methods to [[DataQuanta]] of 2-tuples.
    *
    * @param dataQuanta the [[DataQuanta]]
    */
  implicit class PairDataQuantaWrapper[T: ClassTag, U: ClassTag](dataQuanta: DataQuanta[(T, U)]) {

    /**
      * Plot a bar chart from the [[DataQuanta]]. The tuples are interpreted as `(x, y)` data points.
      *
      * @param title      optional title of the part chart
      * @param xaxisTitle optional title for the x-axis
      * @param yaxisTitle optional title for the y-axis
      * @param ordering   an optional plotting ordering
      * @param publish    adapter for Jupyter-Scala
      */
    def plotBarChart(title: String = null,
                     xaxisTitle: String = null,
                     yaxisTitle: String = null,
                     ordering: Ordering[(T, U)] = null)
                    (implicit publish: Publish): Unit =
      visualizations.plotBarChart(dataQuanta.collect(), title, xaxisTitle, yaxisTitle, ordering)

    /**
      * Plot a scatter chart from the [[DataQuanta]]. The tuples are interpreted as `(x, y)` data points.
      *
      * @param title      optional title of the part chart
      * @param xaxisTitle optional title for the x-axis
      * @param yaxisTitle optional title for the y-axis
      * @param mode       optional [[ScatterMode]] (by default [[ScatterMode.Markers]])
      * @param publish    adapter for Jupyter-Scala
      */
    def plotScatterChart(title: String = null,
                         xaxisTitle: String = null,
                         yaxisTitle: String = null,
                         mode: ScatterMode = ScatterMode(ScatterMode.Markers))
                        (implicit publish: Publish): Unit = {
      val (x, y) = dataQuanta.collect().unzip
      visualizations.plotScatterChart(
        x = Visualizations.asSequence(x),
        y = Visualizations.asSequence(y),
        text = null,
        title = title,
        xaxisTitle = xaxisTitle,
        yaxisTitle = yaxisTitle,
        mode = mode
      )
    }

  }

  /**
    * This class adds additional methods to [[DataQuanta]] of 3-tuples.
    *
    * @param dataQuanta the [[DataQuanta]]
    */
  implicit class TripleDataQuantaWrapper[T: ClassTag, U: ClassTag, V: ClassTag](dataQuanta: DataQuanta[(T, U, V)]) {

    /**
      * Plot a scatter chart from the [[DataQuanta]]. The tuples are interpreted as `(x, y, hover text)` data points.
      *
      * @param title      optional title of the part chart
      * @param xaxisTitle optional title for the x-axis
      * @param yaxisTitle optional title for the y-axis
      * @param mode       optional [[ScatterMode]] (by default [[ScatterMode.Markers]])
      * @param publish    adapter for Jupyter-Scala
      */
    def plotScatterChart(title: String = null,
                         xaxisTitle: String = null,
                         yaxisTitle: String = null,
                         mode: ScatterMode = ScatterMode(ScatterMode.Markers))
                        (implicit publish: Publish): Unit = {
      val (x, y, z) = dataQuanta.collect().unzip3
      visualizations.plotScatterChart(
        x = Visualizations.asSequence(x),
        y = Visualizations.asSequence(y),
        text = z.map(e => if (e == null) null else e.toString).toSeq,
        title = title,
        xaxisTitle = xaxisTitle,
        yaxisTitle = yaxisTitle,
        mode = mode
      )
    }


    /**
      * Plot a heat map from the [[DataQuanta]]. The tuples are interpreted as `(x, y, opacity)` data points.
      *
      * @param width   the width of the heat map
      * @param publish adapter for Jupyter-Scala
      */
    def plotGrid(width: Int = 500, height: Int = 500)(implicit publish: Publish): Unit = {
      val vClass = scala.reflect.classTag[V].runtimeClass
      val vFunction: V => Double =
        if (vClass == classOf[_root_.java.lang.Double]) (v: V) => v.asInstanceOf[_root_.java.lang.Double].doubleValue
        else if (vClass == classOf[Double]) (v: V) => v.asInstanceOf[Double]
        else throw new IllegalArgumentException(s"Unsupported value type: $vClass.")
      val heats = dataQuanta.collect()
      val tiles = heats.map { case (x, y, heat) => Tile(x.toString, y.toString, opacity = vFunction(heat)) }
      visualizations.plotGrid(
        tiles = tiles,
        width = width,
        height = height
      )
    }

  }

  implicit class GraphEdgeDataQuanta(dataQuanta: DataQuanta[GraphEdge]) {

    def plotDirectedGraph(nodes: DataQuanta[GraphVertex] = null,
                          width: String = "100%",
                          height: String = "100%",
                          linkDistance: Int = 100,
                          charge: Int = -300)
                         (implicit publish: Publish): Unit = {
      visualizations.plotDirectedGraph(this.dataQuanta, nodes, width, height, linkDistance, charge)
    }

  }

  implicit class TileDataQuanta(dataQuanta: DataQuanta[Tile]) {

    /**
      * Plot a heat map from the [[DataQuanta]]. The tuples are interpreted as `(x, y, opacity)` data points.
      *
      * @param rows    optional rows for the grid (e.g., to order or project the grid)
      * @param columns optional columns for the grid (e.g., to order or project the grid)
      * @param width   the width of the heat map
      * @param height  the width of the heat map
      * @param publish adapter for Jupyter-Scala
      */
    def plotGrid(rows: DataQuanta[Category] = null,
                 columns: DataQuanta[Category] = null,
                 width: Int = 500,
                 height: Int = 500,
                 marginTop: Int = 200,
                 marginLeft: Int = 200)
                (implicit publish: Publish): Unit = {
      visualizations.plotGrid(
        tiles = dataQuanta,
        rows = rows,
        columns = columns,
        width = width,
        height = height,
        marginLeft = marginLeft,
        marginTop = marginTop
      )
    }


  }

}

