package de.hpi.isg.mdms

import _root_.jupyter.api.Publish
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil
import de.hpi.isg.mdms.model.MetadataStore
import org.qcri.rheem.api.DataQuanta

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

  implicit class PairDataQuantaWrapper[T: ClassTag, U: ClassTag](dataQuanta: DataQuanta[(T, U)]) {

    def plotBarChart(title: String = null,
                     xaxisTitle: String = null,
                     yaxisTitle: String = null)
                    (implicit publish: Publish): Unit =
      visualizations.plotBarChart(dataQuanta.collect(), title, xaxisTitle, yaxisTitle)

  }


}

