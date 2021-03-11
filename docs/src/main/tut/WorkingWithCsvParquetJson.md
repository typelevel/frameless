# Working with CSV and Parquet data

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._

val testDataPath: String = getClass.getResource("/iris.data").getPath
```
Let's start with the basic imports needed for most Frameless projects. 

```tut:silent
import frameless._
import frameless.syntax._
import frameless.functions.aggregate._
```

Here is the schema of the CSV data when read into a Spark Dataframe. 

```tut:book
spark.read.format("csv").load(testDataPath).show(2)
```

The easiest way to read such data into a `TypedDataset` is to create a case class that follows 
the exact number, type, and order for the fields as they appear in the CSV file. This is shown in 
the example bellow with the use of the `Iris` case class. 

```tut:book
final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
val testDataDf = spark.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataDf)
data.show(2).run()
```

```tut:book
final case class Agg(kind: String, sumSLen: Double, sumPLen: Double)

val ag = data.groupBy(data('kind)).agg(sum(data('sLength)), sum(data('pLength))).as[Agg]
ag.show().run()
```

```tut:invisible
spark.stop()
```
