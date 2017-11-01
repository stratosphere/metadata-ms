package de.hpi.isg.mdms.tools

import de.hpi.isg.mdms.domain.constraints.{FunctionalDependency, InclusionDependency, OrderDependency, UniqueColumnCombination}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.targets.{Column, Schema, Target}
import de.hpi.isg.mdms.tools.apps.{CreateSchemaForCsvFilesApp, MetanomeDependencyImportApp, MetanomeStatisticsImportApp}

import scala.reflect.ClassTag

/**
  * This object is simply a Scala-friendly facade for the Java-based import tools.
  */
object Import {

  /**
    * Create a [[Schema]] within a [[MetadataStore]] by inspecting CSV files.
    *
    * @param schemaName     the name of the [[Schema]]
    * @param fileLocation   the location of the CSV files
    * @param fieldSeparator the CSV separator
    * @param quoteChar      the CSV quote character
    * @param hasHeader      whether the CSV files have headers
    * @param sqlFile        an optional SQL file with `CREATE TABLE` statements to load [[Column]] names from
    * @param mds            the Metacrate instance
    */
  def createSchemaFromCsvFiles(schemaName: String,
                               fileLocation: String,
                               fieldSeparator: String = ",",
                               quoteChar: String = "\"",
                               hasHeader: Boolean = false,
                               sqlFile: Option[String] = None)
                              (implicit mds: MetadataStore): Unit = {
    CreateSchemaForCsvFilesApp.fromParameters(mds, fileLocation, schemaName, fieldSeparator, quoteChar, hasHeader, sqlFile.orNull)
  }

  /**
    * Import Metanome statistics into a [[MetadataStore]].
    *
    * @param fileLocation the location of the Metanome statistic file(s)
    * @param schema       the [[Schema]] to which the statistics belong
    * @param filePattern  an optional pattern of Metanome statistic files to include (if `fileLocation` is a directory)
    * @param scope        an optional scope for the created [[ConstraintCollection]]; defaults to the `schema`
    * @param mds          the Metacrate instance
    */
  def importMetanomeStatistics(fileLocation: String,
                               schema: Schema,
                               filePattern: String = ".+",
                               userDefinedIdPrefix: String = null,
                               scope: Target = null)
                              (implicit mds: MetadataStore): Unit = {
    MetanomeStatisticsImportApp.fromParameters(mds,
      fileLocation,
      filePattern,
      schema.getName,
      Option(scope).getOrElse(schema).getName,
      userDefinedIdPrefix)
  }

  /**
    * Import Metanome dependencies into a [[MetadataStore]].
    *
    * @param fileLocation the location of the Metanome dependency file(s)
    * @param schema       the [[Schema]] to which the statistics belong
    * @param filePattern  an optional pattern of Metanome statistic files to include (if `fileLocation` is a directory)
    * @param scope        an optional scope for the created [[ConstraintCollection]]; defaults to the `schema`
    * @param mds          the Metacrate instance
    */
  def importMetanomeDependencies[DependencyType <: Constraint : ClassTag](fileLocation: String,
                                                                          fileType: MetanomeFormat,
                                                                          schema: Schema,
                                                                          filePattern: String = ".+",
                                                                          userDefinedIdPrefix: String = null,
                                                                          scope: Target = null)
                                                                         (implicit mds: MetadataStore): Unit = {
    val dependencyClass = scala.reflect.classTag[DependencyType].runtimeClass
    val dependencyTypeParameter =
      if (dependencyClass == classOf[InclusionDependency]) "IND"
      else if (dependencyClass == classOf[FunctionalDependency]) "FD"
      else if (dependencyClass == classOf[OrderDependency]) "OD"
      else if (dependencyClass == classOf[UniqueColumnCombination]) "UCC"
      else sys.error("Unsupported dependency type.")

    MetanomeDependencyImportApp.fromParameters(mds,
      fileLocation,
      filePattern,
      fileType.name,
      dependencyTypeParameter,
      Option(scope).getOrElse(schema).getName,
      schema.getName,
      userDefinedIdPrefix
    )
  }

  /** Describes a certain Metanome result file format. */
  sealed abstract class MetanomeFormat(val name: String)

  /** Human readable Metanome result format. */
  case object FriendlyMetanomeFormat extends MetanomeFormat("friendly")

  /** Compact Metanome result format. */
  case object CompactMetanomeFormat extends MetanomeFormat("compact")

  /** JSON-based Metanome result format. */
  case object JsonMetanomeFormat extends MetanomeFormat("json")

}
