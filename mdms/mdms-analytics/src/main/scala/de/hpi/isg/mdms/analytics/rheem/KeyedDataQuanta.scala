package de.hpi.isg.mdms.analytics.rheem

import org.qcri.rheem.api.DataQuanta

import scala.reflect.ClassTag

/**
  * This class pimps Rheem's [[DataQuanta]] with additional operations.
  */
class KeyedDataQuanta[Out: ClassTag, Key: ClassTag](val dataQuanta: DataQuanta[Out], val keyExtractor: Out => Key) {

  /**
    * Performs a composite operation consisting of a join and a subsequent assembling of any two matching data quanta
    * into a new output data quantum. The join fields are governed by the [[KeyedDataQuanta]]'s keys.
    *
    * @param that      the other [[KeyedDataQuanta]] to join with
    * @param assembler creates the output data quantum from two joinable data quanta
    * @return the join product [[DataQuanta]]
    */
  def joinAndAssemble[ThatOut: ClassTag, NewOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key], assembler: (Out, ThatOut) => NewOut):
  DataQuanta[NewOut] =
    dataQuanta
      .join[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)
      .map(join => assembler.apply(join.field0, join.field1))

}
