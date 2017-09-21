# Comparing TypedDatasets with Spark's Datasets

```tut:invisible
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.ui.enabled", "false").set("spark.app.id", "tut-dataset")
val spark = SparkSession.builder().config(conf).getOrCreate()

System.clearProperty("spark.master.port")
System.clearProperty("spark.driver.port")
System.clearProperty("spark.hostPort")
System.setProperty("spark.cleaner.ttl", "300")

// We are using this directory so let's make sure it is clean first
org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File("/tmp/foo/"))
```

**Goal:**
  This tutorial compares the standard Spark Datasets api with the one provided by
  frameless' TypedDataset. It shows how TypedDatsets allows for an expressive and
  type-safe api with no compromises on performance.

For this tutorial we first create a simple dataset and save it on disk as a parquet file.
[Parquet](https://parquet.apache.org/) is a popular columnar format and well supported by Spark.
It's important to note that when operating on parquet datasets, Spark knows that each column is stored
separately, so if we only need a subset of the columns Spark will optimize for this and avoid reading
the entire dataset. This is a rather simplistic view of how Spark and parquet work together but it
will serve us well for the context of this discussion.

```tut:book
import spark.implicits._

// Our example case class Foo acting here as a schema
case class Foo(i: Long, j: String)

// Assuming spark is loaded and SparkSession is bind to spark
val initialDs = spark.createDataset( Foo(1, "Q") :: Foo(10, "W") :: Foo(100, "E") :: Nil )

// Assuming you are on Linux or Mac OS
initialDs.write.parquet("/tmp/foo")

val ds = spark.read.parquet("/tmp/foo").as[Foo]

ds.show()
```

The value `ds` holds the content of the `initialDs` read from a parquet file.
Let's try to only use field `i` from Foo and see how Spark's Catalyst (the query optimizer)
optimizes this.

```tut:book
// Using a standard Spark TypedColumn in select()
val filteredDs = ds.filter($"i" === 10).select($"i".as[Long])

filteredDs.show()
```

The `filteredDs` is of type `Dataset[Long]`. Since we only access field `i` from `Foo` the type is correct.
Unfortunately, this syntax requires handholding by explicitly setting the `TypedColumn` in the `select` statement
to return type `Long` (look at the `as[Long]` statement). We will discuss this limitation next in more detail.
Now, let's take a quick look at the optimized Physical Plan that Spark's Catalyst generated.

```tut:book
filteredDs.explain()
```

The last line is very important (see `ReadSchema`). The schema read
from the parquet file only required reading column `i` without needing to access column `j`.
This is great! We have both an optimized query plan and type-safety!

Unfortunately, this syntax is not bulletproof: it fails at run-time if we try to access
a non existing column `x`:


```tut:fail
ds.filter($"i" === 10).select($"x".as[Long])
```

There are two things to improve here. First, we would want to avoid the `at[Long]` casting that we are required
to type for type-safety. This is clearly an area where we can introduce a bug by casting to an incompatible
type. Second, we want a solution where reference to a
non existing column name fails at compilation time.
The standard Spark Dataset can achieve this using the following syntax.

```tut:book
ds.filter(_.i == 10).map(_.i).show()
```

This looks great! It reminds us the familiar syntax from Scala.
The two closures in filter and map are functions that operate on `Foo` and the
compiler will helps us capture all the mistakes we mentioned above.

```tut:fail
ds.filter(_.i == 10).map(_.x).show()
```

Unfortunately, this syntax does not allow Spark to optimize the code.

```tut:book
ds.filter(_.i == 10).map(_.i).explain()
```

As we see from the explained Physical Plan, Spark was not able to optimize our query as before.
Reading the parquet file will required loading all the fields of `Foo`. This might be ok for
small datasets or for datasets with few columns, but will be extremely slow for most practical
applications.
Intuitively, Spark currently doesn't have a way to look inside the code we pass in these two
closures. It only knows that they both take one argument of type `Foo`, but it has no way of knowing if
we use just one or all of `Foo`'s fields.

The TypedDataset in frameless solves this problem. It allows for a simple and type-safe syntax
with a fully optimized query plan.

```tut:book
import frameless.TypedDataset
import frameless.syntax._
val fds = TypedDataset.create(ds)

fds.filter( fds(_.i) === 10 ).select( fds(_.i) ).show().run()
```

And the optimized Physical Plan:

```tut:book
fds.filter( fds(_.i) === 10 ).select( fds(_.i) ).explain()
```

And the compiler is our friend.

```tut:fail
fds.filter( fds(_.i) === 10 ).select( fds(_.x) )
```

## Differences in Encoders

Encoders in Spark's `Datasets` are partially type-safe. If you try to create a `Dataset` using  a type that is not 
 a Scala `Product` then you get a compilation error:

```tut:book
class Bar(i: Int)
```

`Bar` is neither a case class nor a `Product`, so the following correctly gives a compilation error in Spark:

```tut:fail
spark.createDataset(Seq(new Bar(1)))
```

However, the compile type guards implemented in Spark are not sufficient to detect non encodable members. 
For example, using the following case class leads to a runtime failure:

```tut:book
case class MyDate(jday: java.util.Date)
```

```tut:book:fail
val myDateDs = spark.createDataset(Seq(MyDate(new java.util.Date(System.currentTimeMillis))))
```

In comparison, a TypedDataset will notify about the encoding problem at compile time: 

```tut:book:fail
TypedDataset.create(Seq(MyDate(new java.util.Date(System.currentTimeMillis))))
```


```tut:invisible
org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File("/tmp/foo/"))
spark.stop()
```
