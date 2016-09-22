package de.hpi.isg.mdms

import de.hpi.isg.mdms.analytics.rheem.MetadataStoreWrapper
import de.hpi.isg.mdms.model.MetadataStore

import scala.language.implicitConversions

/**
  * Provides utilities to analyze metadata store contents.
  */
package object analytics {

  /**
    * Expose Rheem capabilities on a [[MetadataStore]].
    *
    * @param metadataStore the [[MetadataStore]]
    * @return a [[MetadataStoreWrapper]] exposing Rheem capabilities
    */
  implicit def wrapForRheem(metadataStore: MetadataStore): MetadataStoreWrapper =
  new MetadataStoreWrapper(metadataStore)

}
