# Using Cats with RDDs

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext => SC}
import org.apache.spark.rdd.RDD
val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("cats.bec test")
implicit var sc: SC = new SC(conf)

System.clearProperty("spark.master.port")
System.clearProperty("spark.driver.port")
System.clearProperty("spark.hostPort")
System.setProperty("spark.cleaner.ttl", "300")
```

Data aggregation is one of the most important operations when working with Spark (and data in general).
For example, we often have to compute the `min`, `max`, `avg`, etc. from a set of columns grouped by
different predicates. This section shows how **cats** simplifies these tasks in Spark by
leveraging a large collection of Type Classes for ordering and aggregating data.

All the examples below assume you have previously imported `cats.implicits`.

```tut:book
import cats.implicits._
```

Cats offers ways to sort and aggregate tuples of arbitrary arity.

```tut:book
import frameless.cats.implicits._

val data: RDD[(Int, Int, Int)] = sc.makeRDD((1, 2, 3) :: (1, 5, 3) :: (8, 2, 3) :: Nil)

println(data.csum)
println(data.cmax)
println(data.cmin)
```

The following example aggregates all the elements with a common key.

```tut:book
type User = String
type TransactionCount = Int

val allData: RDD[(User,TransactionCount)] =
   sc.makeRDD(("Bob", 12) :: ("Joe", 1) :: ("Anna", 100) :: ("Bob", 20) :: ("Joe", 2) :: Nil)

val totalPerUser =  allData.csumByKey

totalPerUser.collectAsMap
```

The same example would work for more complex keys.

```tut:book
val allDataComplexKeu =
   sc.makeRDD( ("Bob", Map("task1" -> 10)) ::
    ("Joe", Map("task1" -> 1, "task2" -> 3)) :: ("Bob", Map("task1" -> 10, "task2" -> 1)) :: ("Joe", Map("task3" -> 4)) :: Nil )

val overalTasksPerUser = allDataComplexKeu.csumByKey

overalTasksPerUser.collectAsMap
```

#### Joins

```tut:book
// Type aliases for meaningful types
type TimeSeries = Map[Int,Int]
type UserName = String
```

Example: Using the implicit full-our-join operator

```tut:book
import frameless.cats.outer._

val day1: RDD[(UserName,TimeSeries)] = sc.makeRDD( ("John", Map(0 -> 2, 1 -> 4)) :: ("Chris", Map(0 -> 1, 1 -> 2)) :: ("Sam", Map(0 -> 1)) :: Nil )
val day2: RDD[(UserName,TimeSeries)] = sc.makeRDD( ("John", Map(0 -> 10, 1 -> 11)) :: ("Chris", Map(0 -> 1, 1 -> 2)) :: ("Joe", Map(0 -> 1, 1 -> 2)) :: Nil )

val daysCombined = day1 |+| day2

daysCombined.collect()
```

Note how the user's timeseries from different days have been aggregated together.
The `|+|` (Semigroup) operator for key-value pair RDD will execute a full-outer-join
on the key and combine values using the default Semigroup for the value type.

In `cats`:

```tut:book
Map(1 -> 2, 2 -> 3) |+| Map(1 -> 4, 2 -> -1)
```

```tut:invisible
sc.stop()
```
