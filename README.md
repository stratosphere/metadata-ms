# Metadata Management System

[![Build Status](https://travis-ci.org/stratosphere/metadata-ms.svg?branch=master)](https://travis-ci.org/stratosphere/metadata-ms)

The purpose of this project is to provide a system that stores metadata of relational databases in a convenient way and, hence, treat metadata as first-class data. 

To this end, a specific API is provided for the management of such data. This API is backed by different implementations. As of now, a plain Java serialization as well as a SQLite-based implementation are available.

Later on, an analytic component for the system might follow.
