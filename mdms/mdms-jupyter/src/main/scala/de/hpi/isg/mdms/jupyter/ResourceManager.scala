package de.hpi.isg.mdms.jupyter

import java.io.{BufferedReader, InputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Utility to manage files in the resource folder.
  */
object ResourceManager {

  private val cache = collection.mutable.Map[String, String]()

  /**
    * Provides the given resource and resolves variables.
    *
    * @param resource  the resource to provided
    * @param variables a dictionary of variable values
    * @return the provided resource
    */
  def get(resource: String, variables: Map[String, String] = null): String = {
    var loadedResource = this.cache.getOrElseUpdate(resource, load(resource))
    if (variables != null) {
      for ((identifier, value) <- variables) {
        loadedResource = loadedResource.replaceAllLiterally(s"$$$identifier", value)
      }
    }
    loadedResource
  }


  /**
    * Load the given resource.
    *
    * @param resource the resource to be loaded
    * @return the loaded resource
    */
  private def load(resource: String): String = {
    var reader: BufferedReader = null
    val sb = new StringBuilder
    try {
      reader = new BufferedReader(new InputStreamReader(this.getClass.getResourceAsStream(resource)))
      var line = reader.readLine()
      while (line != null) {
        sb.append(line).append('\n')
        line = reader.readLine()
      }
      sb.toString
    } finally {
      if (reader != null) reader.close()
    }
  }

  /**
    * Provides the given binary resource.
    *
    * @param resource  the resource to provided
    * @return the provided resource
    */
  def getBinary(resource: String): Array[Byte] = {
    var reader: InputStream = null
    val resultBuffer = new ArrayBuffer[Byte]
    try {
      reader = this.getClass.getResourceAsStream(resource)
      val readBuffer = new Array[Byte](1024)
      var readLen: Int = 0
      do {
        readLen = reader.read(readBuffer)
        if (readLen > 0) resultBuffer ++= readBuffer.slice(0, readLen)
      } while (readLen != -1)
    } finally {
      if (reader != null) reader.close()
    }
    resultBuffer.toArray
  }


}
