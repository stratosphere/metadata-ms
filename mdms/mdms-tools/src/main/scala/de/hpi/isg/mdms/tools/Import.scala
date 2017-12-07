package de.hpi.isg.mdms.tools

import de.hpi.isg.mdms.domain.constraints.{FunctionalDependency, InclusionDependency, OrderDependency, UniqueColumnCombination}
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.mdms.model.constraints.{Constraint, ConstraintCollection}
import de.hpi.isg.mdms.model.location.DefaultLocation
import de.hpi.isg.mdms.model.targets.{Column, Schema, Target}
import de.hpi.isg.mdms.tools.apps.{CreateSchemaForCsvFilesApp, MetanomeDependencyImportApp, MetanomeStatisticsImportApp, SqlImportApp}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * This object is simply a Scala-friendly facade for the Java-based import tools.
  */
object Import {

  /**
    * Create a [[Schema]] within a [[MetadataStore]] by inspecting a SQL.
    *
    * @param schemaName        the name of the [[Schema]]
    * @param sqlFileLocation   the location of the SQL file
    * @param schemaDescription an optional description of the [[Schema]]
    * @param store             the [[MetadataStore]]
    */
  def createSchemaFromSqlFile(schemaName: String,
                              sqlFileLocation: String,
                              schemaDescription: String = null)
                             (implicit store: MetadataStore): Schema = {
    val schema = store.addSchema(schemaName, schemaDescription, new DefaultLocation)
    SqlImportApp.importTables(store, schema, Seq(sqlFileLocation))
    schema
  }

  /**
    * Create a [[Schema]] within a [[MetadataStore]] by inspecting CSV files.
    *
    * @param schemaName     the name of the [[Schema]]
    * @param fileLocations   the locations of the CSV files
    * @param fieldSeparator the CSV separator
    * @param quoteChar      the CSV quote character
    * @param hasHeader      whether the CSV files have headers
    * @param sqlFile        an optional SQL file with `CREATE TABLE` statements to load [[Column]] names from
    * @param mds            the Metacrate instance
    */
  def createSchemaFromCsvFiles(schemaName: String,
                               fileLocations: Iterable[String],
                               fieldSeparator: String = ",",
                               quoteChar: String = "\"",
                               hasHeader: Boolean = false,
                               sqlFile: String = null)
                              (implicit mds: MetadataStore): Unit = {
    CreateSchemaForCsvFilesApp.fromParameters(mds, fileLocations, schemaName, fieldSeparator, quoteChar, hasHeader, sqlFile)
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

  /** Imports primary keys from a SQL file into a [[ConstraintCollection]].
    *
    * @param sqlFile       from which the primary key definitions should be read
    * @param schema        to which the SQL file belongs
    * @param userDefinedId an optional user-defined ID for the created [[ConstraintCollection]]
    * @param scope         optional scope for the [[ConstraintCollection]]; defaults to `schema`
    * @param mds           in which the [[ConstraintCollection]] should be created
    */
  def importPrimaryKeyDefinitions(sqlFile: String,
                                  schema: Schema,
                                  userDefinedId: String = null,
                                  scope: Target = null)
                                 (implicit mds: MetadataStore): Unit = {
    val pks = SqlImportApp.loadPrimaryKeys(schema, Seq(sqlFile))
    val constraintCollection = mds.createConstraintCollection(
      userDefinedId,
      s"Primary keys loaded from $sqlFile",
      null,
      classOf[UniqueColumnCombination],
      Option(scope) getOrElse schema
    )
    pks.foreach(constraintCollection.add)
    mds.flush()
  }

  /** Imports foreign keys from a SQL file into a [[ConstraintCollection]].
    *
    * @param sqlFile       from which the foreign key definitions should be read
    * @param schema        to which the SQL file belongs
    * @param userDefinedId an optional user-defined ID for the created [[ConstraintCollection]]
    * @param scope         optional scope for the [[ConstraintCollection]]; defaults to `schema`
    * @param mds           in which the [[ConstraintCollection]] should be created
    */
  def importForeignKeyDefinitions(sqlFile: String,
                                  schema: Schema,
                                  userDefinedId: String = null,
                                  scope: Target = null)
                                 (implicit mds: MetadataStore): Unit = {
    val fks = SqlImportApp.loadForeignKeys(schema, Seq(sqlFile))
    val constraintCollection = mds.createConstraintCollection(
      userDefinedId,
      s"Foreign keys loaded from $sqlFile",
      null,
      classOf[InclusionDependency],
      Option(scope) getOrElse schema
    )
    fks.foreach(constraintCollection.add)
    mds.flush()
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
