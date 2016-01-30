# Metadata Management System 

**Status:** [![Build Status](https://travis-ci.org/stratosphere/metadata-ms.svg?branch=master)](https://travis-ci.org/stratosphere/metadata-ms)

Metadata of relational datasets (e.g., column statistics and inclusion dependencies) are useful for data-oriented tasks, such as query processing, data mining, and data integration. Data profiling techniques (as for instance provided by [Metanome](http://www.metanome.de)) determine such metadata for a given dataset. However, once the metadata have been acquired, they need to be further processed. In particular, it is highly beneficial to integrate and combine the different types of metadata and allow to explore them interactively.

This is where the Metadata Management System (MDMS for short) comes into play. It allows to store metadata in various persistence layers (Java serialization, SQLite, and Cassandra as of now), thereby integrating the different types of metadata. Moreover, MDMS is supposed to complement this persistence layer with an analytical layer, which is to expose a query language and provide various data mining operators to explore the metadata.

## Roadmap

- [x] domain model
- [x] SQLite persistence
- [ ] Cassandra persistence *(some interoperating issues with Flink remain)*
- [ ] File-based persistence: high scalability, build on top of Avro, Parquet or the like
- [x] Data mining operators: K-Means, Apriori
- [x] Primary/foreign key classifiers
- [ ] Query language *(SQL and CQL are usable of course)*
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
