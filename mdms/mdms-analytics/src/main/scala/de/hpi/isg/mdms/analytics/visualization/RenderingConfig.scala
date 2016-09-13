package de.hpi.isg.mdms.analytics.visualization

import de.hpi.isg.mdms.model.MetadataStore

trait RenderingConf {
  def mds: MetadataStore
  def checkInitialization(): Boolean
}

case class RenderingConfig(mds: MetadataStore) extends RenderingConf {
  override def checkInitialization() = true
}

case object UninitializedRenderingConfig extends RenderingConf {
  override def mds = null
  override def checkInitialization() =
    throw UninitializedFieldError(s"${this.getClass.getSimpleName} needs to be initialized with a RenderingConfig!")
}
