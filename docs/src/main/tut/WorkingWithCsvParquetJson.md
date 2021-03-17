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
You need these imports for most Frameless projects. 

```tut:silent
import frameless._
import frameless.syntax._
import frameless.functions.aggregate._
```

We first load some CSV data and print the schema. 

```tut:book
val df = spark.read.format("csv").load(testDataPath)
df.show(2)
df.printSchema
```

The easiest way to read from CSV into a `TypedDataset` is to create a case class that follows 
the exact number, type, and order for the fields as they appear in the CSV file. This is shown in 
the example bellow with the use of the `Iris` case class.

```tut:book
final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
val testDataDf = spark.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataDf)
data.show(2).run()
```

If we do not explicitly define the schema of the CSV file then the types will not match leading to runtime errors. 


```tut:book:fail
val testDataNoSchema = spark.read.format("csv").load(testDataPath)
val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataNoSchema)
data.collect().run()
```

When the dataset has many columns, it is impractical to define a case class that contains all the columns we don't need. 
In such case, we can project the columns we do need, cast them to the proper type, and then call `createUnsafe` using a case class
that contains only the columns we need.  

```tut:book
import org.apache.spark.sql.types.DoubleType
final case class IrisLight(kind: String, sLength: Double)

val testDataDf = spark.read.format("csv").load(testDataPath)
val projectedDf = testDataDf.select(testDataDf("_c4").as("kind"), testDataDf("_c1").cast(DoubleType).as("sLength"))
val data = TypedDataset.createUnsafe[IrisLight](projectedDf)
data.take(2).run()
```


```tut:invisible
spark.stop()
```
