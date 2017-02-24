package de.hpi.isg.mdms.analytics.rheem

import org.qcri.rheem.api.DataQuanta

import scala.reflect.ClassTag

/**
  * This class pimps Rheem's [[DataQuanta]] with additional operations.
  */
class KeyedDataQuanta[Out: ClassTag, Key: ClassTag](val dataQuanta: DataQuanta[Out], val keyExtractor: Out => Key) {

  /**
    * Performs a join. The join fields are governed by the [[KeyedDataQuanta]]'s keys.
    *
    * @param that the other [[KeyedDataQuanta]] to join with
    * @return the join product [[DataQuanta]]
    */
  def keyJoin[ThatOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key]):
  DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out, ThatOut]] =
    dataQuanta.join[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)

}
