# TypedDataset




## Comparing Frameless TypedDatasets with standard Spark Datasets

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

```scala
import spark.implicits._
// import spark.implicits._

// Our example case class Foo acting here as a schema
case class Foo(i: Long, j: String)
// defined class Foo

// Assuming spark is loaded and SparkSession is bind to spark
val initialDs = spark.createDataset( Foo(1, "Q") :: Foo(10, "W") :: Foo(100, "E") :: Nil )
// initialDs: org.apache.spark.sql.Dataset[Foo] = [i: bigint, j: string]

// Assuming you are on Linux or Mac OS
initialDs.write.parquet("/tmp/foo")

val ds = spark.read.parquet("/tmp/foo").as[Foo]
// ds: org.apache.spark.sql.Dataset[Foo] = [i: bigint, j: string]

ds.show()
// +---+---+
// |  i|  j|
// +---+---+
// | 10|  W|
// |100|  E|
// |  1|  Q|
// +---+---+
// 
```

The value `ds` holds the content of the `initialDs` read from a parquet file.
Let's try to only use field `i` from Foo and see how Spark's Catalyst (the query optimizer)
optimizes this.

```scala
// Using a standard Spark TypedColumn in select()
val filteredDs = ds.filter($"i" === 10).select($"i".as[Long])
// filteredDs: org.apache.spark.sql.Dataset[Long] = [i: bigint]

filteredDs.show()
// +---+
// |  i|
// +---+
// | 10|
// +---+
// 
```

The `filteredDs` is of type `Dataset[Long]`. Since we only access field `i` from `Foo` the type is correct.
Unfortunately, this syntax requires handholding by explicitly setting the `TypedColumn` in the `select` statement
to return type `Long` (look at the `as[Long]` statement). We will discuss this limitation next in more detail.
Now, let's take a quick look at the optimized Physical Plan that Spark's Catalyst generated.

```scala
filteredDs.explain()
// == Physical Plan ==
// *Project [i#476L]
// +- *Filter (isnotnull(i#476L) && (i#476L = 10))
//    +- *BatchedScan parquet [i#476L] Format: ParquetFormat, InputPaths: file:/tmp/foo, PartitionFilters: [], PushedFilters: [IsNotNull(i), EqualTo(i,10)], ReadSchema: struct<i:bigint>
```

The last line is very important (see `ReadSchema`). The schema read
from the parquet file only required reading column `i` without needing to access column `j`.
This is great! We have both an optimized query plan and type-safety!

Unfortunately, this syntax is not bulletproof: it fails at run-time if we try to access
a non existing column `x`:


```scala
scala> ds.filter($"i" === 10).select($"x".as[Long])
org.apache.spark.sql.AnalysisException: cannot resolve '`x`' given input columns: [i, j];;
'Project ['x]
+- Filter (i#476L = cast(10 as bigint))
   +- Relation[i#476L,j#477] parquet

  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:77)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:74)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:308)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:308)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:69)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:307)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionUp$1(QueryPlan.scala:269)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$2(QueryPlan.scala:279)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$2$1.apply(QueryPlan.scala:283)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.immutable.List.foreach(List.scala:381)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.immutable.List.map(List.scala:285)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$2(QueryPlan.scala:283)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$8.apply(QueryPlan.scala:288)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:186)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:288)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:74)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:67)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:67)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:58)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:49)
  at org.apache.spark.sql.Dataset.<init>(Dataset.scala:161)
  at org.apache.spark.sql.Dataset.<init>(Dataset.scala:167)
  at org.apache.spark.sql.Dataset.select(Dataset.scala:1023)
  ... 458 elided
```

There are two things to improve here. First, we would want to avoid the `at[Long]` casting that we are required
to type for type-safety. This is clearly an area where we can introduce a bug by casting to an incompatible
type. Second, we want a solution where reference to a
non existing column name fails at compilation time.
The standard Spark Dataset can achieve this using the following syntax.

```scala
ds.filter(_.i == 10).map(_.i).show()
// +-----+
// |value|
// +-----+
// |   10|
// +-----+
// 
```

This looks great! It reminds us the familiar syntax from Scala.
The two closures in filter and map are functions that operate on `Foo` and the
compiler will helps us capture all the mistakes we mentioned above.

```scala
scala> ds.filter(_.i == 10).map(_.x).show()
<console>:23: error: value x is not a member of Foo
       ds.filter(_.i == 10).map(_.x).show()
                                  ^
```

Unfortunately, this syntax does not allow Spark to optimize the code.

```scala
ds.filter(_.i == 10).map(_.i).explain()
// == Physical Plan ==
// *SerializeFromObject [input[0, bigint, true] AS value#512L]
// +- *MapElements <function1>, obj#511: bigint
//    +- *DeserializeToObject newInstance(class $line14.$read$$iw$$iw$$iw$$iw$Foo), obj#510: $line14.$read$$iw$$iw$$iw$$iw$Foo
//       +- *Filter <function1>.apply
//          +- *BatchedScan parquet [i#476L,j#477] Format: ParquetFormat, InputPaths: file:/tmp/foo, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<i:bigint,j:string>
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

```scala
import frameless.TypedDataset
// import frameless.TypedDataset

val fds = TypedDataset.create(ds)
// fds: frameless.TypedDataset[Foo] = [i: bigint, j: string]

fds.filter( fds('i) === 10 ).select( fds('i) ).show().run()
// +---+
// | _1|
// +---+
// | 10|
// +---+
// 
```

And the optimized Physical Plan:

```scala
fds.filter( fds('i) === 10 ).select( fds('i) ).explain()
// == Physical Plan ==
// *Project [i#476L AS _1#568L]
// +- *Filter (isnotnull(i#476L) && (i#476L = 10))
//    +- *BatchedScan parquet [i#476L] Format: ParquetFormat, InputPaths: file:/tmp/foo, PartitionFilters: [], PushedFilters: [IsNotNull(i), EqualTo(i,10)], ReadSchema: struct<i:bigint>
```

And the compiler is our friend.

```scala
scala> fds.filter( fds('i) === 10 ).select( fds('x) )
<console>:25: error: No column Symbol with shapeless.tag.Tagged[String("x")] of type A in Foo
       fds.filter( fds('i) === 10 ).select( fds('x) )
                                               ^
```



