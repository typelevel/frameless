# Typed Encoders in Frameless

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
implicit val sqlContext = spark.sqlContext
System.setProperty("spark.cleaner.ttl", "300")
```

Spark uses Reflection to derive it's `Encoder`s, which is why they can fail at run time. For example, because Spark does not supports `java.util.Date`, the following leads to an error:

```tut
import org.apache.spark.sql.Dataset
import spark.implicits._

case class DateRange(s: java.util.Date, e: java.util.Date)
```

```tut:fail
val ds: Dataset[DateRange] = sqlContext.createDataset(Seq(DateRange(new java.util.Date, new java.util.Date)))
```

As shown by the stack trace, this runtime error goes thought [ScalaReflection](https://github.com/apache/spark/blob/19cf208063f035d793d2306295a251a9af7e32f6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala) to try to derive an `Encoder` for `Dataset` schema. Beside the annoyance of not detecting this error at compile time, a more important limitation of the reflection based approach is it's inability to be extended for custom types. See this Stack Overflow question for a summary of the current situation (as of 2.0) in vanilla Spark: [How to store custom objects in a Dataset?](http://stackoverflow.com/a/39442829/2311362).

Frameless introduces a new type class called `TypeEncoder` to solve these issues. `TypeEncoder`s are passed around as implicit parameters to every frameless method to ensure that the data being manipulated is `Encoder`. It uses a standard implicit resolution coupled with shapeless type class derivation mechanism to ensure every that compiling code manipulates encodable data. For example, the code `java.util.Date` example won't compile with frameless:

```tut
import frameless.TypedDataset
```

```tut:fail
val ds: TypedDataset[DateRange] = TypedDataset.create(Seq(DateRange(new java.util.Date, new java.util.Date)))
```

Type class derivation takes case or recursively constructing (and proving the existence) `TypeEncoder`s for case classes. The following works as expected:

```tut
case class Bar(d: Double, s: String)
case class Foo(i: Int, b: Bar)
val ds: TypedDataset[Foo] = TypedDataset.create(Seq(Foo(1, Bar(1.1, "s"))))
ds.collect()
```

But any non-encodable in the case class hierarchy will be detected at compile time:

```tut:fail
case class BarDate(d: Double, s: String, d: java.util.Date)
case class FooDate(i: Int, b: BarDate)
val ds: TypedDataset[FooDate] = TypedDataset.create(Seq(FooDate(1, BarDate(1.1, "s", new java.util.Date))))
```

It should be noted that once derived, reflection based `Encoder`s and implicitly derived `TypeEncoder`s have identical performances. The derivation mechanism is different, but the objects generated to encode and decode JVM object in the Spark internal representation behave the same at run-time.

```tut:invisible
spark.stop()
```
