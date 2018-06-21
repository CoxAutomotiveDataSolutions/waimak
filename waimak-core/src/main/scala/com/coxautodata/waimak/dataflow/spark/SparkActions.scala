package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowException}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, SaveMode}

/**
  * Defines implicits for functional builders of the data flows.
  * Created by Vicky Avison, Alexei Perelighin and Alex Bush
  */
object SparkActions {

  import SparkActionHelpers._

  /**
    * Defines functional builder for spark specific data flows and common functionalities like reading csv/parquet/hive data,
    * adding spark SQL steps, data set steps, writing data out into various formats, staging and committing multiple outputs into
    * storage like HDFS, Hive/Impala.
    *
    * @param sparkDataFlow
    */
  implicit class SparkDataFlowExtension(sparkDataFlow: SparkDataFlow) extends Logging {

    type returnType = ActionResult[Dataset[_]]

    /**
      * Transforms 1 input DataSet to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param output
      * @param f
      * @return
      */
    def transform(a: String)(output: String)(f: Dataset[_] => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a), List(output), run, "transform 1 -> 1"))
    }

    /**
      * Transforms 2 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String)(output: String)(f: (Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b), List(output), run, "transform 2 -> 1"))
    }

    /**
      * Transforms 3 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String)(output: String)(f: (Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c), List(output), run, "transform 3 -> 1"))
    }

    /**
      * Transforms 4 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String)(output: String)(f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d), List(output), run, "transform 4 -> 1"))
    }

    /**
      * Transforms 5 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String)(output: String)(f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e), List(output), run, "transform 5 -> 1"))
    }

    /**
      * Transforms 6 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String)(output: String)(f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g), List(output), run, "transform 6 -> 1"))
    }

    /**
      * Transforms 7 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String)(output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h), List(output), run, "transform 7 -> 1"))
    }

    /**
      * Transforms 8 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param i
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String, i: String)(output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h), m(i))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h, i), List(output), run, "transform 8 -> 1"))
    }

    /**
      * Transforms 9 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param i
      * @param k
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String, i: String, k: String)(output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h), m(i), m(k))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h, i, k), List(output), run, "transform 9 -> 1"))
    }

    /**
      * Transforms 10 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param i
      * @param k
      * @param l
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String, i: String, k: String, l: String)(output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h), m(i), m(k), m(l))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h, i, k, l), List(output), run, "transform 10 -> 1"))
    }

    /**
      * Transforms 11 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param i
      * @param k
      * @param l
      * @param n
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String, i: String, k: String, l: String, n: String)(output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h), m(i), m(k), m(l), m(n))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h, i, k, l, n), List(output), run, "transform 11 -> 1"))
    }

    /**
      * Transforms 12 input DataSets to 1 output DataSet using function f, which is a scala function.
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param e
      * @param g
      * @param h
      * @param i
      * @param k
      * @param l
      * @param n
      * @param o
      * @param output
      * @param f
      * @return
      */
    def transform(a: String, b: String, c: String, d: String, e: String, g: String, h: String, i: String, k: String, l: String, n: String, o: String)
                 (output: String)
                 (f: (Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_], Dataset[_]) => Dataset[_]): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(f(m(a), m(b), m(c), m(d), m(e), m(g), m(h), m(i), m(k), m(l), m(n), m(o))))

      sparkDataFlow
        .addAction(new SimpleAction(List(a, b, c, d, e, g, h, i, k, l, n, o), List(output), run, "transform 12 -> 1"))
    }

    /**
      * Creates an alias for an existing label, it will point to the same DataSet. This can be used when reading table
      * with one name and saving it with another without any transformations.
      *
      * @param from
      * @param to
      * @return
      */
    def alias(from: String, to: String): SparkDataFlow = {
      def run(m: Map[String, Dataset[_]]): returnType = Seq(Option(m(from)))

      sparkDataFlow
        .addAction(new SimpleAction(List(from), List(to), run, "alias"))
    }

    /**
      * Before writing out data with partition folders, to avoid lots of small files in each folder, DataSet needs
      * to be reshuffled. Optionally it can be sorted as well within each partition.
      *
      * This also can be used if you need to solve problem with Secondary Sort, use mapPartitions on the output.
      *
      * @param input
      * @param output
      * @param partitionCol - columns to repartition/shuffle input data set
      * @param sortCols     - optional sort withing partition columns
      * @return
      */
    def partitionSort(input: String, output: String)(partitionCol: String*)(sortCols: String*): SparkDataFlow = {
      transform(input)(output) { in => in.repartition(partitionCol.map(in(_)): _*).sortWithinPartitions(sortCols.map(in(_)): _*)
      }
    }

    /**
      * Adds actions that prints to console first 10 lines of the input. Useful for debug and development purposes.
      *
      * @param label
      * @return
      */
    def show(label: String): SparkDataFlow = sparkDataFlow
      .addAction(new SimpleAction(List(label), List.empty, _.get(label).fold(Seq.empty) { d =>
        println("Dump: " + label) //did not work properly with logInfo, as show does not use logger to print to console.
        d.show(false)
        Seq.empty
      }, "show"))

    /**
      * Prints DataSet's schema to console.
      *
      * @param label
      * @return
      */
    def printSchema(label: String): SparkDataFlow = sparkDataFlow
      .addAction(new SimpleAction(List(label), List.empty, _.get(label).fold(Seq.empty) { d =>
        println("Schema: " + label) //did not work properly with logInfo, as printSchema does not use logger to print to console.
        d.printSchema()
        Seq.empty
      }, "printSchema"))

    /**
      * Opens multiple Hive/Impala tables. Table names become waimak lables, which can be prefixed.
      *
      * @param dbName       - name of the database that contains the table
      * @param outputPrefix - optional prefix for the waimak label
      * @param tables       - list of table names in Hive/Impala that will also become waimak labels
      * @return
      */
    def openTable(dbName: String, outputPrefix: Option[String] = None)(tables: String*): SparkDataFlow = {
      if (tables.isEmpty) throw new DataFlowException("At least one table name must be provided.")

      val outputLabels = tables.map(n => s"${outputPrefix.map(p => s"${p}_").getOrElse("")}${n}").toList

      def read(): returnType = {
        val res: returnType = tables.map(table => s"select * from ${dbName}.${table}")
          .map(sql => Some(sparkDataFlow.spark.sql(sql))).toList
        res
      }

      sparkDataFlow.addAction(new SimpleAction(List.empty, outputLabels, _ => read(), "openTable"))
    }

    /**
      * A generic action to open a dataset with a given label by providing a function that maps from
      * a [[SparkFlowContext]] object to a Dataset.
      * In most cases the user should use a more specialised open fucntion
      *
      * @param label Label of the resulting dataset
      * @param open  Function that maps from a [[SparkFlowContext]] object to a Dataset.
      * @return
      */
    def open(label: String, open: SparkFlowContext => Dataset[_]): SparkDataFlow = {
      openBase(sparkDataFlow, label)(open)
    }

    /**
      * A generic action to open a dataset with a given label by providing a function that maps from
      * a DataFrameReader object to a Dataset.
      * In most cases the user should use a more specialised open fucntion
      *
      * @param label Label of the resulting dataset
      * @param open  Function that maps from a DataFrameReader object to a Dataset.
      * @return
      */
    def open(label: String, open: DataFrameReader => Dataset[_], options: Map[String, String]): SparkDataFlow = {
      openBase(sparkDataFlow, label)(applyOpenDataFrameReader andThen applyReaderOptions(options) andThen open)
    }

    /**
      * Opens multiple DataSets directly on the folders in the basePath folder. Folders with names must exist in the
      * basePath and the respective data sets will be inputs of the flow with the same names. It is also possible to
      * specify prefix for the output labels: Ex name is "table1" and prefix is "test" then output label will be
      * "test_table1".
      *
      * In case of generated models as inputs, they will have a snapshot folder, which is the same across all models in
      * the path. Use snapshotFolder to isolate the data for a single snapshot.
      *
      * Ex:
      * /path/to/tables/table1/snapshot_key=2018_02_12_10_59_21
      * /path/to/tables/table1/snapshot_key=2018_02_13_10_00_09
      * /path/to/tables/table2/snapshot_key=2018_02_12_10_59_21
      * /path/to/tables/table2/snapshot_key=2018_02_13_10_00_09
      *
      * There are 2 snapshots of the table1 and table2 tables. To access just one of the snapshots:
      *
      * basePath = /path/to/tables
      * names = Seq("table1", "table2")
      * snapshotFolder = Some("snapshot_key=2018_02_13_10_00_09")
      * outputPrefix = None
      *
      * This will add 2 inputs to the data flow "table1", "table2", without a prefix as prefix is None.
      *
      * @param basePath       Base path of all the labels
      * @param snapshotFolder Optional snapshot folder (including key and value as key=value)
      * @param outputPrefix   Optional prefix to attach to the flow labels
      * @param labels         List of labels to open
      * @param open           - function that given a string can produce a function that takes a DataFrameReader and produces a Dataset
      * @return
      */
    def open(basePath: String, snapshotFolder: Option[String], outputPrefix: Option[String],
             labels: Seq[String])(open: String => DataFrameReader => Dataset[_]): SparkDataFlow = {

      labels.foldLeft(sparkDataFlow) {
        (currentFlow, label) =>
          val outputLabel = outputPrefix.map(p => s"${p}_$label").getOrElse(label)
          val labelPath = s"$basePath/$label${snapshotFolder.map(s => s"/$s").getOrElse("")}"
          openBase(currentFlow, outputLabel)(applyOpenDataFrameReader andThen open(labelPath))
      }

    }

    /**
      * Open a CSV file based on a complete path
      *
      * @param path    Complete path of the CSV file(s) (can include glob)
      * @param label   Label to attach to the dataset
      * @param options Options for the DataFrameReader
      * @return
      */
    def openFileCSV(path: String, label: String, options: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")): SparkDataFlow = {
      openBase(sparkDataFlow, label)(applyOpenDataFrameReader andThen applyReaderOptions(options) andThen applyOpenCSV(path))
    }

    /**
      * Open a Parquet path based on a complete path
      *
      * @param path    Complete path of the parquet file(s) (can include glob)
      * @param label   Label to attach to the dataset
      * @param options Options for the DataFrameReader
      * @return
      */
    def openFileParquet(path: String, label: String, options: Map[String, String] = Map()): SparkDataFlow = {
      openBase(sparkDataFlow, label)(applyOpenDataFrameReader andThen applyReaderOptions(options) andThen applyOpenParquet(path))
    }

    /**
      * Opens parquet based folders using open(). See parent function for complete description.
      *
      * @param basePath       Base path of all the labels
      * @param snapshotFolder Optional snapshot folder below table folder
      * @param outputPrefix   Optional prefix to attach to the dataset label
      * @param options        Options for the DataFrameReader
      * @param labels         List of labels/folders to open
      * @return
      */
    def openParquet(basePath: String, snapshotFolder: Option[String] = None, outputPrefix: Option[String] = None, options: Map[String, String] = Map())(labels: String*): SparkDataFlow = {
      open(basePath, snapshotFolder, outputPrefix, labels)(path => applyReaderOptions(options) andThen applyOpenParquet(path))
    }

    /**
      * Opens CSV folders as data sets. See parent function for complete description.
      *
      * @param basePath       Base path of all the labels
      * @param snapshotFolder Optional snapshot folder below table folder
      * @param outputPrefix   Optional prefix to attach to the dataset label
      * @param options        Options for the DataFrameReader
      * @param labels         List of labels/folders to open
      * @return
      */
    def openCSV(basePath: String, snapshotFolder: Option[String] = None, outputPrefix: Option[String] = None, options: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true"))(labels: String*): SparkDataFlow = {
      open(basePath, snapshotFolder, outputPrefix, labels)(path => applyReaderOptions(options) andThen applyOpenCSV(path))

    }

    /**
      * Executes Spark sql. All input labels must already be registered as sql tables using tempView* functions.
      * Output is automatically added as temp view.
      *
      * @param inputs      - required inputs, used mainly for scheduling.
      * @param sqlQuery    - sql code that uses registered labels as table names
      * @param outputLabel - label of the output transformation
      * @param dropColumns - optional list of columns to drop after transformation
      * @return
      */
    def sql(input: String, inputs: String*)(outputLabel: String, sqlQuery: String, dropColumns: String*): SparkDataFlow = {
      logInfo("SQL query: " + sqlQuery)

      def run(dfs: Map[String, Dataset[_]]): returnType = {
        val sqlRes = sparkDataFlow.spark.sql(sqlQuery)
        val res = dropColumns.foldLeft(sqlRes) { (dr, colName) => dr.drop(colName) }
        Seq(Option(res))
      }

      val sqlTables = (input +: inputs).toList
      sparkDataFlow.addAction(new SparkSimpleAction(sqlTables, List(outputLabel), d => run(d), sqlTables, "sql"))
    }

    /**
      * Base function for all write operation on current data flow, in most of the cases users should use more specialised one.
      *
      * @param label - label whose data set will be written out
      * @param pre   - dataset transformation function
      * @param dfr   - dataframe writer function
      */
    def write(label: String, pre: Dataset[_] => Dataset[_], dfr: DataFrameWriter[_] => Unit): SparkDataFlow = {
      writeBase(sparkDataFlow, label)(pre)(dfr)
    }


    /**
      * Writes out data set as parquet, can have partitioned columns.
      *
      * @param label            - label whose data set will be written out
      * @param repartition      - repartition dataframe on partition columns
      * @param basePath         - base path of the label, label will be added to it
      * @param partitionColumns - optional list of partition columns, which will become partition folders
      * @return
      */
    def writePartitionedParquet(basePath: String, repartition: Boolean = true)(label: String, partitionColumns: String*): SparkDataFlow = {
      write(label, applyRepartition(partitionColumns, repartition), applyPartitionBy(partitionColumns) andThen applyWriteParquet(new Path(basePath, label).toString))
    }

    /**
      * Writes multiple datasets as parquet files into basePath. Names of the labels will become names of the folders
      * under the basePath.
      *
      * @param basePath  - path in which folders will be created
      * @param overwrite - if true than overwrite the existing data. By default it is false
      * @param labels    - labels to write as parquets, labels will become folder names
      * @return
      */
    def writeParquet(basePath: String, overwrite: Boolean = false)(labels: String*): SparkDataFlow = {
      if (labels.isEmpty) throw new DataFlowException("writeParquet requires at least one label")
      labels.foldLeft(sparkDataFlow) {
        (resFlow, label) =>
          resFlow.write(label, df => df, applyOverwrite(overwrite) andThen applyWriteParquet(new Path(basePath, label).toString))
      }
    }

    /**
      * Writes out data set as csv, can have partitioned columns.
      *
      * @param basePath         - base path of the label, label will be added to it
      * @param repartition      - repartition dataframe on partition columns
      * @param options          - list of options to apply to the dataframewriter
      * @param label            - label whose data set will be written out
      * @param partitionColumns - optional list of partition columns, which will become partition folders
      * @return
      */
    def writePartitionedCSV(basePath: String, repartition: Boolean = true, options: Map[String, String] = Map.empty)(label: String, partitionColumns: String*): SparkDataFlow = {
      write(label, applyRepartition(partitionColumns, repartition), applyWriterOptions(options) andThen applyPartitionBy(partitionColumns) andThen applyWriteCSV(new Path(basePath, label).toString))
    }

    /**
      * Writes out data set as csv.
      *
      * @param basePath  - path in which folders will be created
      * @param labels    - labels whose data set will be written out
      * @param options   - list of options to apply to the dataframewriter
      * @param overwrite - whether to overwrite existing data
      * @param numFiles  - number of files to produce as output
      * @return
      */
    def writeCSV(basePath: String, options: Map[String, String] = Map.empty, overwrite: Boolean = false, numFiles: Option[Int] = Some(1))(labels: String*): SparkDataFlow = {
      labels.foldLeft(sparkDataFlow)((flow, label) => {
        flow.write(label, applyFileReduce(numFiles), applyWriterOptions(options) andThen applyOverwrite(overwrite) andThen applyWriteCSV(new Path(basePath, label).toString))
      })
    }

    /**
      * Stage a dataframe into the temporary folder. All staged dataframes will then be committed
      * at the same time at the end of the flow.
      *
      * You can optionally give a snapshot folder name so resulting committed table looks like:
      * /destBasePath/label/snapshotFolder/
      *
      * @param destBasePath   Base path
      * @param labels         Labels of the dataframe to stage
      * @param snapshotFolder Optional snapshot name
      * @param partitions     Optionally partition the output
      * @param repartition    Optionally repartition by partition columns
      * @return
      */
    def stageAndCommitParquet(destBasePath: String, snapshotFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, repartition: Boolean = true)(labels: String*): SparkDataFlow = {
      labels.foldLeft(sparkDataFlow)((flow, label) => {
        flow.cacheAsPartitionedParquet(partitions, repartition)(label)
          .addCommitLabel(label, LabelCommitDefinition(destBasePath, snapshotFolder))
      })
    }

    /**
      * Stage a dataframe into the temporary folder. All staged dataframes will then be committed
      * at the same time at the end of the flow and then committed to a Hadoop DB.
      * A table will be created if it does not exist.
      *
      * You can optionally give a snapshot folder name so resulting committed table looks like:
      * /destBasePath/label/snapshotFolder/
      *
      * @param connection     Hadoop database connection object
      * @param destBasePath   Base path
      * @param labels         Labels of the dataframe to stage, resulting in the tablename
      * @param snapshotFolder Optional snapshot name
      * @param partitions     Optionally partition the output
      * @param repartition    Optionally repartition by partition columns
      * @return
      */
    def stageAndCommitParquetToDB(connection: HadoopDBConnector)(destBasePath: String, snapshotFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, repartition: Boolean = true)(labels: String*): SparkDataFlow = {
      labels.foldLeft(sparkDataFlow)((flow, label) => {
        flow.cacheAsPartitionedParquet(partitions, repartition)(label)
          .addCommitLabel(label, LabelCommitDefinition(destBasePath, snapshotFolder, partitions, Some(connection)))
      })
    }

    /**
      * In zeppelin it is easier to debug and visualise data as spark sql tables. This action does no data transformations,
      * it only marks labels as SQL tables. Only after execution of the flow it is possible
      *
      * @param labels - labels to mark.
      * @return
      */
    def debugAsTable(labels: String*): SparkDataFlow = {
      sparkDataFlow.addAction(new SparkSimpleAction(labels.toList, List.empty, _ => Seq.empty, labels, "debugAsTable"))
    }
  }


  implicit class SparkInterceptorActions(sparkDataFlow: SparkDataFlow) extends Logging {

    /**
      * Creates a persistent snapshot into the staging folder of the spark data flow and substitutes the dataset
      * behind the label with the one opened from the stored version.
      *
      * It will not trigger for labels whose datasets are empty.
      *
      * @param labels - list of labels to snapshot
      * @return
      */
    def cacheAsParquet(labels: String*): SparkDataFlow = {
      if (labels.isEmpty) throw new DataFlowException(s"At least one label must be specified for cacheAsParquet")

      labels.foldLeft(sparkDataFlow) { (flow, label) => SparkInterceptors.addCacheAsParquet(flow, label) }
    }

    /**
      * Creates a persistent snapshot into the staging folder of the spark data flow and substitutes the dataset
      * behind the label with the one opened from the stored version.
      *
      * It will not trigger for labels whose datasets are empty.
      *
      * @param labels - list of labels to snapshot
      * @return
      */
    def cacheAsPartitionedParquet(partitions: Seq[String], repartition: Boolean = true)(labels: String*): SparkDataFlow = {
      if (labels.isEmpty) throw new DataFlowException(s"At least one label must be specified for cacheAsParquet")

      labels.foldLeft(sparkDataFlow) { (flow, label) => SparkInterceptors.addCacheAsParquet(flow, label, partitions, repartition) }
    }

    /**
      * Applies a transformation to the label's data set and replaces it.
      *
      * Multiple intercept action can be chained. Like post -> post -> snapshot.
      *
      * @param label
      * @param post
      * @return
      */
    def inPlaceTransform(label: String)(post: Dataset[_] => Dataset[_]): SparkDataFlow = SparkInterceptors.addPostTransform(sparkDataFlow, label)(post)

  }

}

object SparkActionHelpers {

  /**
    * Base function for all write operation, in most of the cases users should use more specialised one.
    * This one is used by other builders.
    *
    * @param dataFlow - flow to which to add the write action
    * @param label    - label whose data set will be written out
    * @param pre      - dataset transformation function
    * @param dfr      - dataframe writer function
    * @return
    */
  def writeBase(dataFlow: SparkDataFlow, label: String)(pre: Dataset[_] => Dataset[_])(dfr: DataFrameWriter[_] => Unit): SparkDataFlow = {
    def run(m: Map[String, Dataset[_]]): ActionResult[Dataset[_]] = {
      dfr(pre(m(label)).write)
      Seq.empty
    }

    dataFlow.addAction(new SimpleAction(List(label), List.empty, run, "write"))
  }

  def applyWriterOptions(options: Map[String, String]): DataFrameWriter[_] => DataFrameWriter[_] = {
    writer => options.foldLeft(writer)((z, c) => z.option(c._1, c._2))
  }

  def applyRepartition(partitionCols: Seq[String], repartition: Boolean): Dataset[_] => Dataset[_] = {
    ds => if (repartition && partitionCols.nonEmpty) ds.repartition(partitionCols.map(ds(_)): _*) else ds
  }

  def applyFileReduce(numFiles: Option[Int]): Dataset[_] => Dataset[_] = {
    ds => numFiles.map(ds.repartition).getOrElse(ds)
  }

  def applyPartitionBy(partitionCols: Seq[String]): DataFrameWriter[_] => DataFrameWriter[_] = {
    dfw => if (partitionCols.nonEmpty) dfw.partitionBy(partitionCols: _*) else dfw
  }

  def applyMode(mode: SaveMode): DataFrameWriter[_] => DataFrameWriter[_] = dfw => dfw.mode(mode)

  def applyOverwrite(overwrite: Boolean): DataFrameWriter[_] => DataFrameWriter[_] = {
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    applyMode(mode)
  }

  def applyWriteParquet(path: String): DataFrameWriter[_] => Unit = dfw => dfw.parquet(path)

  def applyWriteCSV(path: String): DataFrameWriter[_] => Unit = dfw => dfw.csv(path)

  /**
    * Base function for all read operation, in all cases users should use more specialised one.
    * This one is used by other builders.
    *
    * @param dataFlow - flow to which to add the write action
    * @param label    - Label of the output Dataset
    * @param open     - dataset opening function
    * @return
    */
  def openBase(dataFlow: SparkDataFlow, label: String)(open: SparkFlowContext => Dataset[_]): SparkDataFlow = {
    def read(): ActionResult[Dataset[_]] = {
      Seq(Some(open(dataFlow.flowContext)))
    }

    dataFlow.addAction(new SimpleAction(List.empty, List(label), _ => read(), "read"))
  }

  def applyOpenDataFrameReader: SparkFlowContext => DataFrameReader = {
    flowContext => flowContext.spark.read
  }

  def applyReaderOptions(options: Map[String, String]): DataFrameReader => DataFrameReader = {
    reader => options.foldLeft(reader)((z, c) => z.option(c._1, c._2))
  }

  def applyOpenCSV(path: String): DataFrameReader => Dataset[_] = {
    reader => reader.csv(path)
  }

  def applyOpenParquet(path: String): DataFrameReader => Dataset[_] = {
    reader => reader.parquet(path)
  }
}