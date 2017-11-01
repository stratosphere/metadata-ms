package de.hpi.isg.mdms.jupyter.json

/**
  * Utilities to serialize objects as JSON.
  */
object JsonSerializer {

  def escape(str: String) = str.replaceAll("\"", "\\\"")

  def toJson(obj: JsonSerializable) = Option(obj) match {
    case None => "null"
    case Some(o) => o.toJson
  }

  def toJson(array: Traversable[JsonSerializable]): String = {
    val sb = new StringBuilder
    sb.append('[')
    var separator = ""
    array.foreach { element =>
      sb.append(separator).append(toJson(element))
      separator = ", "
    }
    sb.append(']').toString()
  }

}
