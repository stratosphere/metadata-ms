package de.hpi.isg.mdms.jupyter.json

import scala.language.implicitConversions

/**
  * Utilities to serialize objects as JSON.
  */
object JsonSerializer {

  def escape(str: String) = str.replace("\"", "\\\"").replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")

  implicit def toJson(obj: JsonSerializable) = Option(obj) match {
    case None => "null"
    case Some(o) => o.toJson
  }

  implicit def stringToJson(str: String): String =
    if (str == null) null
    else s""""${escape(str)}""""

  def toJson[T](array: Traversable[T])(implicit serializeElements: T => String): String = {
    val sb = new StringBuilder
    sb.append('[')
    var separator = ""
    array.foreach { element =>
      sb.append(separator).append(serializeElements(element))
      separator = ", "
    }
    sb.append(']').toString()
  }


}
