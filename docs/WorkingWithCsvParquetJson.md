# Working with CSV and Parquet data

```scala mdoc:invisible:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._

val testDataPath: String = "docs/iris.data"
```
You need these imports for most Frameless projects. 

```scala mdoc:silent
import frameless._
import frameless.syntax._
import frameless.functions.aggregate._
```

## Working with CSV 

We first load some CSV data and print the schema. 

```scala mdoc
val df = spark.read.format("csv").load(testDataPath)
df.show(2)
df.printSchema
```

The easiest way to read from CSV into a `TypedDataset` is to create a case class that follows 
the exact number, type, and order for the fields as they appear in the CSV file. This is shown in 
the example bellow with the use of the `Iris` case class.

```scala mdoc
final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
val testDataDf = spark.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataDf)
data.show(2).run()
```

If we do not explicitly define the schema of the CSV file then the types will not match leading to runtime errors. 

```scala mdoc:nest
val testDataNoSchema = spark.read.format("csv").load(testDataPath)
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataNoSchema)
```

```scala mdoc:crash
data.collect().run()
```

### Dealing with CSV files with multiple columns

When the dataset has many columns, it is impractical to define a case class that contains many columns we don't need. 
In such case, we can project the columns we do need, cast them to the proper type, and then call `createUnsafe` using a case class
that contains a much smaller subset of the columns.  

```scala mdoc:nest
import org.apache.spark.sql.types.DoubleType
final case class IrisLight(kind: String, sLength: Double)

val testDataDf = spark.read.format("csv").load(testDataPath)
val projectedDf = testDataDf.select(testDataDf("_c4").as("kind"), testDataDf("_c1").cast(DoubleType).as("sLength"))
val data = TypedDataset.createUnsafe[IrisLight](projectedDf)
data.take(2).run()
```

```scala mdoc:invisible
spark.stop()
```

## Working with Parquet
```scala mdoc:invisible:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._

val testDataPathParquet: String = "docs/iris.parquet"
import frameless._
import frameless.syntax._
import frameless.functions.aggregate._

final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
```

Spark is much better at reading the schema from parquet files. 

```scala mdoc
val testDataParquet = spark.read.format("parquet").load(testDataPathParquet)
testDataParquet.printSchema
```

So as long as we use a type (case class) that reflects the same number, type, and order of the fields 
from the data everything works as expected. 

```scala mdoc:nest
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataParquet)
data.take(2).run()
```

### Dealing with Parquet files with multiple columns

The main difference compared to CSV is that with Parquet Spark is better at inferring the types. This makes it simpler 
to project the columns we need without having the cast the to the proper type. 

```scala mdoc:nest
final case class IrisLight(kind: String, sLength: Double)

val projectedDf = testDataParquet.select("kind", "sLength")
val data = TypedDataset.createUnsafe[IrisLight](projectedDf)
data.take(2).run()
```

```scala mdoc:invisible
spark.stop()
```
