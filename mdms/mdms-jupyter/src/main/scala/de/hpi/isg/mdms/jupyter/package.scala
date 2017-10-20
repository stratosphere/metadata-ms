package de.hpi.isg.mdms

import _root_.jupyter.api.Publish
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil
import de.hpi.isg.mdms.model.MetadataStore

/**
  * This package object provides the Metacrate-speficif API for Jupyter.
  */
package object jupyter {

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
    * Print the Metacrate logo.
    */
  def printLogo()(implicit publish: Publish): Unit = {
    val logo = ResourceManager.getBinary("/metacrate-logo.png")
    publish.png(logo)
  }

}

