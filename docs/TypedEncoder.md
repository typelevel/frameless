# Typed Encoders in Frameless

```scala mdoc:invisible:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
System.setProperty("spark.cleaner.ttl", "300")
```

Spark uses Reflection to derive its `Encoder`s, which is why they can fail at run time. For example, because Spark does not support `java.util.Date`, the following leads to an error:

```scala mdoc:silent
import org.apache.spark.sql.Dataset
import spark.implicits._

case class DateRange(s: java.util.Date, e: java.util.Date)
```

```scala mdoc:crash
val ds: Dataset[DateRange] = Seq(DateRange(new java.util.Date, new java.util.Date)).toDS()
```

As shown by the stack trace, this runtime error goes through [ScalaReflection](https://github.com/apache/spark/blob/19cf208063f035d793d2306295a251a9af7e32f6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala) to try to derive an `Encoder` for `Dataset` schema. Beside the annoyance of not detecting this error at compile time, a more important limitation of the reflection-based approach is its inability to be extended for custom types. See this Stack Overflow question for a summary of the current situation (as of 2.0) in vanilla Spark: [How to store custom objects in a Dataset?](http://stackoverflow.com/a/39442829/2311362).

Frameless introduces a new type class called `TypeEncoder` to solve these issues. `TypeEncoder`s are passed around as implicit parameters to every Frameless method to ensure that the data being manipulated is `Encoder`. It uses a standard implicit resolution coupled with shapeless' type class derivation mechanism to ensure every that compiling code manipulates encodable data. For example, the `java.util.Date` example won't compile with Frameless:

```scala mdoc:silent
import frameless.TypedDataset
import frameless.syntax._
```

```scala mdoc:fail
val ds: TypedDataset[DateRange] = TypedDataset.create(Seq(DateRange(new java.util.Date, new java.util.Date)))
```

Type class derivation takes care of recursively constructing (and proving the existence of) `TypeEncoder`s for case classes. The following works as expected:

```scala mdoc
case class Bar(d: Double, s: String)
case class Foo(i: Int, b: Bar)
val ds: TypedDataset[Foo] = TypedDataset.create(Seq(Foo(1, Bar(1.1, "s"))))
ds.collect()
```

But any non-encodable in the case class hierarchy will be detected at compile time:

```scala mdoc:silent
case class BarDate(d: Double, s: String, t: java.util.Date)
case class FooDate(i: Int, b: BarDate)
```

```scala mdoc:fail
val ds: TypedDataset[FooDate] = TypedDataset.create(Seq(FooDate(1, BarDate(1.1, "s", new java.util.Date))))
```

It should be noted that once derived, reflection-based `Encoder`s and implicitly derived `TypeEncoder`s have identical performance. The derivation mechanism is different, but the objects generated to encode and decode JVM objects in Spark's internal representation behave the same at runtime.

```scala mdoc:invisible
spark.stop()
```
