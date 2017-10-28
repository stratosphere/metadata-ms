package de.hpi.isg.mdms.jupyter.json

/**
  * Declares that a class can be converted into a JSON representation.
  */
trait JsonSerializable {

  /**
    * Transform this instance into a JSON string.
    * @return the JSON string
    */
  def toJson: String

}
