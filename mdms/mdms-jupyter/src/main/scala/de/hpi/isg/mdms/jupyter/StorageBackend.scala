package de.hpi.isg.mdms.jupyter

/**
  * Storage backend for a [[de.hpi.isg.mdms.model.MetadataStore]].
  */
trait StorageBackend

case class SqliteBackend(location: String) extends StorageBackend

case class JavaSerializedBackend(location: String) extends StorageBackend

case object CassandraBackend extends StorageBackend

case object InMemoryBackend extends StorageBackend


