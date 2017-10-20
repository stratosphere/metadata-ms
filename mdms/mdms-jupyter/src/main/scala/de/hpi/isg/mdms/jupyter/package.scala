package de.hpi.isg.mdms

import _root_.jupyter.api.Publish;

/**
  * This package object provides the Metacrate-speficif API for Jupyter.
  */
package object jupyter {

  /**
    * Print the Metacrate logo.
    */
  def printLogo()(implicit publish: Publish): Unit = {
//    publish.html("""<img src="https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/projekte/Metacrate/metacrate-logo.png">""")
    val logo = ResourceManager.getBinary("/metacrate-logo.png")
    publish.png(logo)
  }

}
