# Metadata Management System 

**Status:** [![Build Status](https://travis-ci.org/stratosphere/metadata-ms.svg?branch=master)](https://travis-ci.org/stratosphere/metadata-ms)

Metadata of relational datasets (e.g., column statistics and inclusion dependencies) are useful for data-oriented tasks, such as query processing, data mining, and data integration. Data profiling techniques (as for instance provided by [Metanome](http://www.metanome.de)) determine such metadata for a given dataset. However, once the metadata have been acquired, they need to be further processed. In particular, it is highly beneficial to integrate and combine the different types of metadata and allow to explore them interactively.

This is where the Metadata Management System (MDMS for short) comes into play. It allows to store metadata in various persistence layers (Java serialization, SQLite, and Cassandra as of now), thereby integrating the different types of metadata. Moreover, MDMS is supposed to complement this persistence layer with an analytical layer, which is to expose a query language and provide various data mining operators to explore the metadata.

## Usage notes

Besides providing a library for metadata management, the MDMS provides a set of utilities that can be run from the command line and allow for the management of metadata stores. Note that all tools can be run without parameters to explain their usage.

**Create a metadata store.** Creating and initializing a metadata store is the first step. This metadata store can later on manage metadata. To create a new metadata store, run the main class `de.hpi.isg.mdms.tools.apps.CreateMetadataStoreApp`.

**Import a database schema.** We provide a tool to automatically extract the basic schema information (tables, columns) of a database that is represented by CSV files. Importing such a schema is necessary (i) to configure data profiling algorithms appropriately, e.g., to define a set of schema elements to be profiled, and (ii) to integrate various metadata types by having them referencing the imported schema elements. To import a schema from a set of CSV files, run the main class `de.hpi.isg.mdms.tools.apps.CreateSchemaForCsvFilesApp`. 

**Fill the metadata store.** To extract metadata from databases is the task of data profiling tools. This issue is orthogonal to the goals of the MDMS, which aims at managing metadata but not their discovery. While in general the MDMS offers APIs to interact with data profiling algorithms, we also provide a tool to import metadata from the [Metanome](http://www.metanome.de) data profiling tool. To do so, run the main classes `de.hpi.isg.mdms.tools.apps.MetanomeDependencyImportApp` (for functional dependencies, inclusion dependencies, and unique column combinations) and `de.hpi.isg.mdms.tools.apps.MetanomeStatisticsImportApp` (for column statistics).

**Analyze metadata.** This phase is currently in development. Some preview functionality can be found in the main classes `de.hpi.isg.mdms.java.apps.PrimaryKeyClassifier` and `de.hpi.isg.mdms.java.apps.ForeignKeyClassifier` (for PK and FK classification) and `de.hpi.isg.mdms.flink.apps.KmeansUccsApp` and `de.hpi.isg.mdms.flink.apps.AprioriUccsApp` (for data mining on unique column combinations).

**Use a client.** The MDMS currently offers two interfaces. The first one is a CLI and offers all of the above described functionality. Just run the main class `de.hpi.isg.mdms.cli.apps.MDMSCliApp`. Moreover, this CLI can also be used via [Apache Zeppelin](https://zeppelin.incubator.apache.org/). Check out [metadata-ms-on-zeppelin](https://github.com/stratosphere/metadata-ms-on-zeppelin). 

## Roadmap

- [x] domain model
- [x] SQLite persistence
- [ ] Cassandra persistence *(some interoperating issues with Flink remain)*
- [ ] File-based persistence: high scalability, build on top of Avro, Parquet or the like
- [x] Data mining operators: K-Means, Apriori
- [x] Primary/foreign key classifiers
- [ ] Query language/analytical layer *(SQL and CQL are usable of course)*
- [ ] Integration with Metanome *(results can be imported, but algorithms cannot be triggered)*
- [x] Provide a CLI as most basic MDMS operator
- [ ] Frontend

## Project overview

* Base modules
  * `mdms-model`: metamodel of relational schemata
  * `mdms-dependencies`: metamodel of most common dependencies (e.g., inclusion dependencies and functional dependencies)
  * `mdms-util`: general-purpose utilities used throughout the project
* Persistence modules
  * `mdms-simple`: persistence using Java serialization
  * `mdms-rdmbs`: abstract persistence module for relational databases
  * `mdms-sqlite`: presistence with SQLite
  * `mdms-cassandra`: persistence with Cassandra
* Application modules
  * `mdms-clients`: utilities to write MDMS-based applications
  * `mdms-tools`: basic MDMS applications, such as importing a schema from CSV files into a metadata store
  * `mdms-java`: Java-based utilities for MDMS applications
  * `mdms-flink`: Flink-based utilites for MDMS applications (complementary to `mdms-java`)
  * `mdms-cli`: CLI-based client to operate the metadata store
