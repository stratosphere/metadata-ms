package de.hpi.isg.mdms.tools

import de.hpi.isg.mdms.domain.constraints.{Signature, TableSample, Vector}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.ConstraintCollection
import de.hpi.isg.mdms.model.targets.{Schema, Target}
import de.hpi.isg.mdms.tools.apps.{CreateQGramSketchApp, CreateSampleApp}

import scala.collection.JavaConversions._

/**
  * This object offers a Scala facade to access Metacrate's profiling capabilities.
  */
object Profiling {

  /**
    * Profile all tables of a [[Schema]] for q-grams and store them in a [[ConstraintCollection]].
    *
    * @param schema     the [[Schema]] whose (CSV) tables should be profiled
    * @param sampleSize the maximum size of tuples to sample from each table
    * @param seed       the seed for the randomization
    * @param scope      of the [[ConstraintCollection]]; defaults to `schema`
    * @param store      within which the `schema` resides
    */
  def profileSamples(schema: Schema,
                     userDefinedId: String = null,
                     sampleSize: Int = 100,
                     seed: Int = 42,
                     scope: Target = null)
                    (implicit store: MetadataStore): Unit = {
    val samples = CreateSampleApp.profileSamples(store, schema, sampleSize, seed)
    val constraintCollection = store.createConstraintCollection(
      userDefinedId,
      s"Table samples for schema ${schema.getName}",
      null,
      classOf[TableSample],
      if (scope != null) scope else schema
    )
    samples.foreach(constraintCollection.add)
    store.flush()
  }

  /**
    * Profile all tables of a [[Schema]] for q-grams and store them in a [[ConstraintCollection]].
    *
    * @param schema           the [[Schema]] whose (CSV) tables should be profiled
    * @param numDimensions    the number of dimensions of the signature vectors
    * @param useDoubleHashing whether to hash every q-gram only twice and derive all hashes as a linear combination of the two
    * @param q                the size of the q-grams
    * @param scope            of the [[ConstraintCollection]]; defaults to `schema`
    * @param store            within which the `schema` resides
    */
  def profileQGramSignatures(schema: Schema,
                             userDefinedId: String = null,
                             numDimensions: Int = 128,
                             useDoubleHashing: Boolean = true,
                             q: Int = 2,
                             scope: Target = null)
                            (implicit store: MetadataStore): Unit = {
    val signatures = CreateQGramSketchApp.profileQGramSignatures(store, schema, numDimensions, useDoubleHashing, q)
    val constraintCollection = store.createConstraintCollection(
      userDefinedId,
      s"Q-gram sketches profiled for schema ${schema.getName}",
      null,
      classOf[Signature],
      if (scope != null) scope else schema
    )
    signatures.foreach(constraintCollection.add)
    store.flush()
  }

  /**
    * Profile all tables of a [[Schema]] for q-grams and store them in a [[ConstraintCollection]].
    *
    * @param schema              the [[Schema]] whose (CSV) tables should be profiled
    * @param numSketchDimensions the number of dimensions to sketch the space of q-grams
    * @param numQGramDimensions  the dimensionality of the resulting q-gram [[Vector]]s
    * @param seed                to create random transformation matrix from the sketch space to the q-gram vector space
    * @param q                   the size of the q-grams
    * @param scope               of the [[ConstraintCollection]]; defaults to `schema`
    * @param store               within which the `schema` resides
    */
  def profileQGramSketches(schema: Schema,
                           userDefinedId: String = null,
                           numSketchDimensions: Int = 16,
                           numQGramDimensions: Int = 1024,
                           seed: Int = 42,
                           q: Int = 2,
                           scope: Target = null)
                          (implicit store: MetadataStore): Unit = {
    val qGramSketches = CreateQGramSketchApp.profileQGramSketches(store, schema, numSketchDimensions, numQGramDimensions, seed, q)
    val constraintCollection = store.createConstraintCollection(
      userDefinedId,
      s"Q-gram sketches profiled for schema ${schema.getName}",
      null,
      classOf[Vector],
      if (scope != null) scope else schema
    )
    qGramSketches.foreach(constraintCollection.add)
    store.flush()
  }

}
