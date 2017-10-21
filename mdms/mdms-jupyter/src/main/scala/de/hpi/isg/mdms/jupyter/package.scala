package de.hpi.isg.mdms

import _root_.jupyter.api.Publish
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil
import de.hpi.isg.mdms.model.MetadataStore
import org.qcri.rheem.api.DataQuanta
import plotly.element.ScatterMode

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
      * @param publish    adapter for Jupyter-Scala
      */
    def plotBarChart(title: String = null,
                     xaxisTitle: String = null,
                     yaxisTitle: String = null)
                    (implicit publish: Publish): Unit =
      visualizations.plotBarChart(dataQuanta.collect(), title, xaxisTitle, yaxisTitle)

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

  }


}

