# Waimak

[![Build Status](https://travis-ci.org/CoxAutomotiveDataSolutions/waimak.svg?branch=develop)](https://travis-ci.org/CoxAutomotiveDataSolutions/waimak) 
[![Maven Central](https://img.shields.io/maven-central/v/com.coxautodata/waimak-core_2.11.svg)](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak*) [![Coverage Status](https://img.shields.io/codecov/c/github/CoxAutomotiveDataSolutions/waimak/develop.svg)](https://codecov.io/gh/CoxAutomotiveDataSolutions/waimak/branch/develop) [![Join the chat at https://gitter.im/waimak-framework/users](https://badges.gitter.im/waimak-framework/users.svg)](https://gitter.im/waimak-framework/users?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img align="right" src="./images/waimak.svg">

## What is Waimak?

Waimak is an open-source framework that makes it easier to create complex data flows in Apache Spark.

Waimak aims to abstract the more complex parts of Spark application development (such as orchestration) away from the business logic, allowing users to get their business logic in a production-ready state much faster. By using a framework written by Data Engineers, the teams defining the business logic can write and own their production code.

Our metaphor to describe this framework is the braided river – it splits and rejoins to itself repeatedly on its journey. By describing a Spark application as a sequence of flow transformations, Waimak can execute independent branches of the flow in parallel making more efficient use of compute resources and greatly reducing the execution time of complex flows.

## Why would I use Waimak?
We developed Waimak to:
* allow teams to own their own business logic without owning an entire production Spark application
* reduce the time it takes to write production-ready Spark applications
* provide an intuitive structure to Spark applications by describing them as a sequence of transformations forming a flow
* increase the performance of Spark data flows by making more efficient use of the Spark executors

Importantly, Waimak is a framework for building Spark applications by describing a sequence of composed Spark transformations. To create those transformations Waimak exposes the complete Spark API, giving you the power of Apache Spark with added structure.

## How do I get started?

You can import Waimak into your Maven project using the following dependency details:

```xml
        <dependency>
            <groupId>com.coxautodata</groupId>
            <artifactId>waimak-core_2.11</artifactId>
            <version>${waimak.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
```

Waimak marks the Spark dependency as optional so as not to depend on any specific release of Spark, therefore you must specify the version of Spark you wish to use as a dependency. Waimak _should_ run on any version of Spark 2.2+, however the list of officially tested versions is given below.

The following code snippet demonstrates a basic Waimak example taken from the unit tests:

```scala
// Required imports
import com.coxautodata.waimak.dataflow.Waimak

// Initialise basic Waimak objects
val emptyFlow = Waimak.sparkFlow(spark)

// Add actions to the flow
val basicFlow = emptyFlow
    .openCSV(basePath)("csv_1", "csv_2")
    .alias("csv_1", "items")
    .alias("csv_2", "person")
    .writeParquet(baseDest)("items", "person")

// Run the flow
basicFlow.execute()
```

This example is very small, but in practice flow definitions can become very large depending of the number of inputs and outputs in a job.

The project wiki page provides best practices for structuring your project when dealing with large flows.

## What Waimak modules are available?

Waimak currently consists of the following modules:

Artifact ID | Purpose | Maven Release
----------- | ------- | -------------
`waimak-core` | Core Waimak functionality and generic actions | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-core*) 
`waimak-configuration-databricks` | Databricks-specific configuration provider using secret scopes (Scala 2.11 only) | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-configuration-databricks*)
`waimak-impala` | Impala implementation of the `HadoopDBConnector` used for commiting labels to an Impala DB | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-impala*)
`waimak-hive` | Hive implementation of the `HadoopDBConnector` used for commiting labels to a Hive Metastore | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-hive*)
`waimak-rdbm-ingestion` | Functionality to ingest inputs from a range of RDBM sources | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-rdbm-ingestion*)
`waimak-storage` | Functionality for providing a hot/cold region-based ingestion storage layer | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-storage*)
`waimak-app` | Functionality providing Waimak application templates and orchestration | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-app*)
`waimak-experimental` | Experimental features currently under development | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-experimental*)
`waimak-dataquality` | Functionality for monitoring and alerting on data quality | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-dataquality*)
`waimak-deequ` | Amazon Deequ implementation of data quality monitoring (Scala 2.11 only) | [Maven Central](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:waimak-deequ*)

## What versions of Spark are supported?

Waimak is tested against the following versions of Spark:

Package Maintainer | Spark Version | Scala Version
------------------ | ------------- | -------------
Apache Spark | [2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html) | 2.11
Apache Spark | [2.3.0](https://spark.apache.org/releases/spark-release-2-3-0.html) | 2.11
Apache Spark | [2.4.0](https://spark.apache.org/releases/spark-release-2-4-0.html) | 2.11
Apache Spark | [2.4.3](https://spark.apache.org/releases/spark-release-2-4-3.html) | 2.12
Cloudera Spark | [2.2.0](https://www.cloudera.com/documentation/spark2/latest/topics/spark2.html) | 2.11

Other versions of Spark >= 2.2 are also likely to work and can be added to the list of tested versions if there is sufficient need.

## Where can I learn more?

You can find the latest documentation for Waimak on the [project wiki page](https://github.com/CoxAutomotiveDataSolutions/waimak/wiki). This README file contains basic setup instructions and general project information.

You can also find details of what's in the latest releases in the [changelog](CHANGELOG.md).

Finally, you can also talk to the developers and other users directly at our [Gitter room](https://gitter.im/waimak-framework/users).

## Can I contribute to Waimak?

We welcome all users to contribute to the development of Waimak by raising pull-requests. We kindly ask that you include suitable unit tests along with proposed changes.

### How do I test my contributions?

Waimak is tested against different versions of Spark 2.x to ensure uniform compatibility. The versions of Spark tested by Waimak are given in the `<profiles>` section of the POM. You can activate a given profile in the POM by using the `-P` flag: `mvn clean package -P apache-2.3.0_2.11`

The integration tests of the RDBM ingestion module require Docker therefore you must have the Docker service running and the current user must be able to access the Docker service.

## What is Waimak licensed under?

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright 2018 Cox Automotive UK Limited
