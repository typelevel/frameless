# TypedDataset support for Spark ML

The goal of the `frameless-ml` module is to be able to use Spark ML with `TypedDataset` and
to eventually provide a more strongly typed ML API for Spark. Currently, this module is at its very beginning and only 
provides `TypedEncoder` instances for Spark ML's linear algebra data types.

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
implicit val sqlContext = spark.sqlContext
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```
 
## Using Vector and Matrix with TypedDataset

`frameless-ml` provides `TypedEncoder` instances for `org.apache.spark.ml.linalg.Vector` 
and `org.apache.spark.ml.linalg.Matrix`:

```tut:book
import frameless.ml._
import frameless.TypedDataset
import org.apache.spark.ml.linalg._

val vector = Vectors.dense(1, 2, 3)
val vectorDs = TypedDataset.create(Seq("label" -> vector))

val matrix = Matrices.dense(2, 1, Array(1, 2))
val matrixDs = TypedDataset.create(Seq("label" -> matrix))
```

Under the hood, Vector and Matrix are encoded using `org.apache.spark.ml.linalg.VectorUDT` 
and `org.apache.spark.ml.linalg.MatrixUDT`. This is possible thanks to the implicit derivation 
from `org.apache.spark.sql.types.UserDefinedType[A]` to `TypedEncoder[A]` defined in `TypedEncoder` companion object.

```tut:invisible
spark.stop()
```