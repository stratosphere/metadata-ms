package de.hpi.isg.mdms.analytics.rheem

import org.qcri.rheem.api.DataQuanta

import scala.reflect.ClassTag

/**
  * This class pimps Rheem's joined [[DataQuanta]] with additional operations.
  */
class JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]
(val dataQuanta: DataQuanta[org.qcri.rheem.basic.data.Tuple2[Out0, Out1]]) {

  /**
    * Assembles a new element from a join product tuple.
    *
    * @param assembler creates the output data quantum from two joinable data quanta
    * @return the join product [[DataQuanta]]
    */
  def assemble[NewOut: ClassTag](assembler: (Out0, Out1) => NewOut):
  DataQuanta[NewOut] =
    dataQuanta.map(join => assembler.apply(join.field0, join.field1))

}
