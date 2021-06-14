# TypedDataset: Feature Overview

This tutorial introduces `TypedDataset` using a simple example.
The following imports are needed to make all code examples compile.

```scala mdoc:silent:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
import frameless.TypedDataset

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```

## Creating TypedDataset instances

We start by defining a case class:

```scala mdoc:silent
case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
```

And few `Apartment` instances:

```scala mdoc:silent
val apartments = Seq(
  Apartment("Paris", 50,  300000.0, 2),
  Apartment("Paris", 100, 450000.0, 3),
  Apartment("Paris", 25,  250000.0, 1),
  Apartment("Lyon",  83,  200000.0, 2),
  Apartment("Lyon",  45,  133000.0, 1),
  Apartment("Nice",  74,  325000.0, 3)
)
```

We are now ready to instantiate a `TypedDataset[Apartment]`:

```scala mdoc
val aptTypedDs = TypedDataset.create(apartments)
```

We can also create one from an existing Spark `Dataset`:

```scala mdoc:nest
val aptDs = spark.createDataset(apartments)
val aptTypedDs = TypedDataset.create(aptDs)
```

Or use the Frameless syntax:

```scala mdoc
import frameless.syntax._

val aptTypedDs2 = aptDs.typed
```

## Typesafe column referencing
This is how we select a particular column from a `TypedDataset`:

```scala mdoc
val cities: TypedDataset[String] = aptTypedDs.select(aptTypedDs('city))
```

This is completely type-safe, for instance suppose we misspell `city` as `citi`:

```scala mdoc:fail
aptTypedDs.select(aptTypedDs('citi))
```

This gets raised at compile time, whereas with the standard `Dataset` API the error appears at runtime (enjoy the stack trace):

```scala mdoc:crash
aptDs.select('citi)
```

`select()` supports arbitrary column operations:

```scala mdoc
aptTypedDs.select(aptTypedDs('surface) * 10, aptTypedDs('surface) + 2).show().run()
```

Note that unlike the standard Spark API, where some operations are lazy and some are not, **all TypedDatasets operations are lazy.**
In the above example, `show()` is lazy. It requires to apply `run()` for the `show` job to materialize.
A more detailed explanation of `Job` is given [here](Job.md).

Next we compute the price by surface unit:

```scala mdoc:fail
val priceBySurfaceUnit = aptTypedDs.select(aptTypedDs('price) / aptTypedDs('surface))
```

As the error suggests, we can't divide a `TypedColumn` of `Double` by `Int.`
For safety, in Frameless only math operations between same types is allowed:

```scala mdoc
val priceBySurfaceUnit = aptTypedDs.select(aptTypedDs('price) / aptTypedDs('surface).cast[Double])
priceBySurfaceUnit.collect().run()
```

Looks like it worked, but that `cast` seems unsafe right? Actually it is safe.
Let's try to cast a `TypedColumn` of `String` to `Double`:

```scala mdoc:fail
aptTypedDs('city).cast[Double]
```

The compile-time error tells us that to perform the cast, an evidence
(in the form of `CatalystCast[String, Double]`) must be available.
Since casting from `String` to `Double` is not allowed, this results
in a compilation error.

Check [here](https://github.com/typelevel/frameless/blob/master/core/src/main/scala/frameless/CatalystCast.scala)
for the set of available `CatalystCast.`

## Working with Optional columns

When working with real data we have to deal with imperfections, such as missing fields. Columns that may have
missing data should be represented using `Options`. For this example, let's assume that the Apartments dataset
may have missing values.  

```scala mdoc:silent
case class ApartmentOpt(city: Option[String], surface: Option[Int], price: Option[Double], bedrooms: Option[Int])
```

```scala mdoc:silent
val apartmentsOpt = Seq(
  ApartmentOpt(Some("Paris"), Some(50),  Some(300000.0), None),
  ApartmentOpt(None, None, Some(450000.0), Some(3))
)
```

```scala mdoc
val aptTypedDsOpt = TypedDataset.create(apartmentsOpt)
aptTypedDsOpt.show().run()
```

Unfortunately the syntax used above with `select()` will not work here:

```scala mdoc:fail
aptTypedDsOpt.select(aptTypedDsOpt('surface) * 10, aptTypedDsOpt('surface) + 2).show().run()
```

This is because we cannot multiple an `Option` with an `Int`. In Scala, `Option` has a `map()` method to help address
exactly this (e.g., `Some(10).map(c => c * 2)`). Frameless follows a similar convention. By applying the `opt` method on 
any `Option[X]` column you can then use `map()` to provide a function that works with the unwrapped type `X`. 
This is best shown in the example bellow:

 ```scala mdoc
 aptTypedDsOpt.select(aptTypedDsOpt('surface).opt.map(c => c * 10), aptTypedDsOpt('surface).opt.map(_ + 2)).show().run()
 ```

**Known issue**: `map()` will throw a runtime exception when the applied function includes a `udf()`. If you want to 
apply a `udf()` to an optional column, we recommend changing your `udf` to work directly with `Optional` fields. 


## Casting and projections

In the general case, `select()` returns a TypedDataset of type `TypedDataset[TupleN[...]]` (with N in `[1...10]`).
For example, if we select three columns with types `String`, `Int`, and `Boolean` the result will have type
`TypedDataset[(String, Int, Boolean)]`. 

We often want to give more expressive types to the result of our computations.
`as[T]` allows us to safely cast a `TypedDataset[U]` to another of type `TypedDataset[T]` as long
as the types in `U` and `T` align.

When the cast is valid the expression compiles:

```scala mdoc
case class UpdatedSurface(city: String, surface: Int)
val updated = aptTypedDs.select(aptTypedDs('city), aptTypedDs('surface) + 2).as[UpdatedSurface]
updated.show(2).run()
```

Next we try to cast a `(String, String)` to an `UpdatedSurface` (which has types `String`, `Int`).
The cast is not valid and the expression does not compile:

```scala mdoc:fail
aptTypedDs.select(aptTypedDs('city), aptTypedDs('city)).as[UpdatedSurface]
```

### Advanced topics with `select()`

When you `select()` a single column that has type `A`, the resulting type is `TypedDataset[A]` and 
not `TypedDataset[Tuple1[A]]`. This behavior makes working with nested schema easier (i.e., in the case 
where `A` is a complex data type) and simplifies type-checking column operations (e.g., verify that two 
columns can be added, divided, etc.). However, when `A` is scalar, say a `Long`, it makes it harder to select 
and work with the resulting `TypedDataset[Long]`. For instance, it's harder to reference this single scalar 
column using `select()`. If this becomes an issue, you can bypass this behavior by using the 
`selectMany()` method instead of `select()`. In the previous example, `selectMany()` will return
`TypedDataset[Tuple1[Long]]` and you can reference its single column using the name `_1`. 
`selectMany()` should also be used when you need to select more than 10 columns. 
`select()` has better IDE support and compiles faster than the macro based `selectMany()`, 
so prefer `select()` for the most common use cases.

When you are handed a single scalar column TypedDataset (e.g., `TypedDataset[Double]`) 
the best way to reference its single column is using the `asCol` (short for "as a column") method. 
This is best shown in the example below. We will see more usages of `asCol` later in this tutorial.  

```scala mdoc:nest
val priceBySurfaceUnit = aptTypedDs.select(aptTypedDs('price) / aptTypedDs('surface).cast[Double])
priceBySurfaceUnit.select(priceBySurfaceUnit.asCol * 2).show(2).run()
```


### Projections

We often want to work with a subset of the fields in a dataset.
Projections allow us to easily select our fields of interest
while preserving their initial names and types for extra safety.

Here is an example using the `TypedDataset[Apartment]` with an additional column:

```scala mdoc
val aptds = aptTypedDs // For shorter expressions

case class ApartmentDetails(city: String, price: Double, surface: Int, ratio: Double)
val aptWithRatio =
  aptds.select(
    aptds('city),
    aptds('price),
    aptds('surface),
    aptds('price) / aptds('surface).cast[Double]
  ).as[ApartmentDetails]
```

Suppose we only want to work with `city` and `ratio`:

```scala mdoc
case class CityInfo(city: String, ratio: Double)

val cityRatio = aptWithRatio.project[CityInfo]

cityRatio.show(2).run()
```

Suppose we only want to work with `price` and `ratio`:

```scala mdoc
case class PriceInfo(ratio: Double, price: Double)

val priceInfo = aptWithRatio.project[PriceInfo]

priceInfo.show(2).run()
```

We see that the order of the fields does not matter as long as the
names and the corresponding types agree. However, if we make a mistake in
any of the names and/or their types, then we get a compilation error.

Say we make a typo in a field name:

```scala mdoc:silent
case class PriceInfo2(ratio: Double, pricEE: Double)
```

```scala mdoc:fail
aptWithRatio.project[PriceInfo2]
```

Say we make a mistake in the corresponding type:

```scala mdoc:silent
case class PriceInfo3(ratio: Int, price: Double) // ratio should be Double
```

```scala mdoc:fail
aptWithRatio.project[PriceInfo3]
```

### Union of TypedDatasets 

Lets create a projection of our original dataset with a subset of the fields.

```scala mdoc:nest:silent
case class ApartmentShortInfo(city: String, price: Double, bedrooms: Int)

val aptTypedDs2: TypedDataset[ApartmentShortInfo] = aptTypedDs.project[ApartmentShortInfo]
```

The union of `aptTypedDs2` with `aptTypedDs` uses all the fields of the caller (`aptTypedDs2`)
and expects the other dataset (`aptTypedDs`) to include all those fields. 
If field names/types do not match you get a compilation error. 

```scala mdoc
aptTypedDs2.union(aptTypedDs).show().run
```

The other way around will not compile, since `aptTypedDs2` has only a subset of the fields. 

```scala mdoc:fail
aptTypedDs.union(aptTypedDs2).show().run
```

Finally, as with `project`, `union` will align fields that have same names/types,
so fields do not have to be in the same order. 

## TypedDataset functions and transformations

Frameless supports many of Spark's functions and transformations. 
However, whenever a Spark function does not exist in Frameless, 
calling `.dataset` will expose the underlying 
`Dataset` (from org.apache.spark.sql, the original Spark APIs), 
where you can use anything that would be missing from the Frameless' API.

These are the main imports for Frameless' aggregate and non-aggregate functions.

```scala
import frameless.functions._                // For literals
import frameless.functions.nonAggregate._   // e.g., concat, abs
import frameless.functions.aggregate._      // e.g., count, sum, avg 
```

### Drop/Replace/Add fields

`dropTupled()` drops a single column and results in a tuple-based schema.

```scala mdoc
aptTypedDs2.dropTupled('price): TypedDataset[(String,Int)]
```

To drop a column and specify a new schema use `drop()`.

```scala mdoc
case class CityBeds(city: String, bedrooms: Int)
val cityBeds: TypedDataset[CityBeds] = aptTypedDs2.drop[CityBeds] 
```

Often, you want to replace an existing column with a new value.
 
```scala mdoc
val inflation = aptTypedDs2.withColumnReplaced('price, aptTypedDs2('price) * 2)
 
inflation.show(2).run()
```

Or use a literal instead.

```scala mdoc
import frameless.functions.lit
aptTypedDs2.withColumnReplaced('price, lit(0.001)) 
```

Adding a column using `withColumnTupled()` results in a tupled-based schema.

```scala mdoc
aptTypedDs2.withColumnTupled(lit(Array("a","b","c"))).show(2).run()
```

Similarly, `withColumn()` adds a column and explicitly expects a schema for the result.

```scala mdoc
case class CityBedsOther(city: String, bedrooms: Int, other: List[String])

cityBeds.
   withColumn[CityBedsOther](lit(List("a","b","c"))).
   show(1).run()
```

To conditionally change a column use the `when/otherwise` operation. 

```scala mdoc
import frameless.functions.nonAggregate.when
aptTypedDs2.withColumnTupled(
   when(aptTypedDs2('city) === "Paris", aptTypedDs2('price)).
   when(aptTypedDs2('city) === "Lyon", lit(1.1)).
   otherwise(lit(0.0))).show(8).run()
```

A simple way to add a column without losing important schema information is
to project the entire source schema into a single column using the `asCol()` method.

```scala mdoc
val c = cityBeds.select(cityBeds.asCol, lit(List("a","b","c")))
c.show(1).run()
```

When working with Spark's `DataFrames`, you often select all columns using `.select($"*", ...)`. 
In a way, `asCol()` is a typed equivalent of `$"*"`. 

To access nested columns, use the `colMany()` method. 

```scala mdoc
c.select(c.colMany('_1, 'city), c('_2)).show(2).run()
```

### Working with collections


```scala mdoc
import frameless.functions._
import frameless.functions.nonAggregate._
```

```scala mdoc
val t = cityRatio.select(cityRatio('city), lit(List("abc","c","d")))
t.withColumnTupled(
   arrayContains(t('_2), "abc")
).show(1).run()
```

If accidentally you apply a collection function on a column that is not a collection,
you get a compilation error.

```scala mdoc:fail
t.withColumnTupled(
   arrayContains(t('_1), "abc")
)
```

Flattening columns in Spark is done with the `explode()` method. Unlike vanilla Spark, 
in Frameless `explode()` is part of `TypedDataset` and not a function of a column. 
This provides additional safety since more than one `explode()` applied in a single 
statement results in runtime error in vanilla Spark.   


```scala mdoc
val t2 = cityRatio.select(cityRatio('city), lit(List(1,2,3,4)))
val flattened = t2.explode('_2): TypedDataset[(String, Int)]
flattened.show(4).run()
```

Here is an example of how `explode()` may fail in vanilla Spark. The Frameless 
implementation does not suffer from this problem since, by design, it can only be applied
to a single column at a time. 

```scala mdoc:fail
{
  import org.apache.spark.sql.functions.{explode => sparkExplode}
  t2.dataset.toDF().select(sparkExplode($"_2"), sparkExplode($"_2"))
}
```



### Collecting data to the driver

In Frameless all Spark actions (such as `collect()`) are safe.

Take the first element from a dataset (if the dataset is empty return `None`).

```scala mdoc
cityBeds.headOption.run()
```

Take the first `n` elements.

```scala mdoc
cityBeds.take(2).run()
```

```scala mdoc
cityBeds.head(3).run()
```

```scala mdoc
cityBeds.limit(4).collect().run()
```

## Sorting columns


Only column types that can be sorted are allowed to be selected for sorting. 

```scala mdoc
aptTypedDs.orderBy(aptTypedDs('city).asc).show(2).run()
```

The ordering can be changed by selecting `.acs` or `.desc`. 

```scala mdoc
aptTypedDs.orderBy(
   aptTypedDs('city).asc, 
   aptTypedDs('price).desc
).show(2).run()
```


## User Defined Functions

Frameless supports lifting any Scala function (up to five arguments) to the
context of a particular `TypedDataset`:

```scala mdoc:nest
// The function we want to use as UDF
val priceModifier =
    (name: String, price:Double) => if(name == "Paris") price * 2.0 else price

val udf = aptTypedDs.makeUDF(priceModifier)

val aptds = aptTypedDs // For shorter expressions

val adjustedPrice = aptds.select(aptds('city), udf(aptds('city), aptds('price)))

adjustedPrice.show().run()
```

## GroupBy and Aggregations
Let's suppose we wanted to retrieve the average apartment price in each city
```scala mdoc
val priceByCity = aptTypedDs.groupBy(aptTypedDs('city)).agg(avg(aptTypedDs('price)))
priceByCity.collect().run()
```
Again if we try to aggregate a column that can't be aggregated, we get a compilation error
```scala mdoc:fail
aptTypedDs.groupBy(aptTypedDs('city)).agg(avg(aptTypedDs('city)))
```

Next, we combine `select` and `groupBy` to calculate the average price/surface ratio per city:

```scala mdoc:nest
val aptds = aptTypedDs // For shorter expressions

val cityPriceRatio =  aptds.select(aptds('city), aptds('price) / aptds('surface).cast[Double])

cityPriceRatio.groupBy(cityPriceRatio('_1)).agg(avg(cityPriceRatio('_2))).show().run()
```

We can also use `pivot` to further group data on a secondary column.
For example, we can compare the average price across cities by number of bedrooms.

```scala mdoc
case class BedroomStats(
   city: String,
   AvgPriceBeds1: Option[Double], // Pivot values may be missing, so we encode them using Options
   AvgPriceBeds2: Option[Double],
   AvgPriceBeds3: Option[Double],
   AvgPriceBeds4: Option[Double])

val bedroomStats = aptds.
   groupBy(aptds('city)).
   pivot(aptds('bedrooms)).
   on(1,2,3,4). // We only care for up to 4 bedrooms
   agg(avg(aptds('price))).
   as[BedroomStats]  // Typesafe casting

bedroomStats.show().run()
```

With pivot, collecting data preserves typesafety by
encoding potentially missing columns with `Option`.

```scala mdoc
bedroomStats.collect().run().foreach(println)
```

#### Working with Optional fields

Optional fields can be converted to non-optional using `getOrElse()`. 

```scala mdoc
val sampleStats = bedroomStats.select(
   bedroomStats('AvgPriceBeds2).getOrElse(0.0),
   bedroomStats('AvgPriceBeds3).getOrElse(0.0))

sampleStats.show().run()   
``` 

In addition, optional columns can be flatten using the `.flattenOption` method on `TypedDatset`.
The result contains the rows for which the flattened column is not None (or null). The schema
is automatically adapted to reflect this change.

```scala mdoc
val flattenStats = bedroomStats.flattenOption('AvgPriceBeds2)


// The second Option[Double] is now of type Double, since all 'null' values are removed
flattenStats: TypedDataset[(String, Option[Double], Double, Option[Double], Option[Double])]
```

In a DataFrame, if you just ignore types, this would equivelantly be written as:

```scala mdoc
bedroomStats.dataset.toDF().filter($"AvgPriceBeds2".isNotNull)
```


### Entire TypedDataset Aggregation

We often want to aggregate the entire `TypedDataset` and skip the `groupBy()` clause.
In Frameless you can do this using the `agg()` operator directly on the `TypedDataset`.
In the following example, we compute the average price, the average surface,
the minimum surface, and the set of cities for the entire dataset.

```scala mdoc
case class Stats(
   avgPrice: Double,
   avgSurface: Double,
   minSurface: Int,
   allCities: Vector[String])

aptds.agg(
   avg(aptds('price)),
   avg(aptds('surface)),
   min(aptds('surface)),
   collectSet(aptds('city))
).as[Stats].show().run()
```

You may apply any `TypedColumn` operation to a `TypedAggregate` column as well.

```scala mdoc
import frameless.functions._
aptds.agg(
   avg(aptds('price)) * min(aptds('surface)).cast[Double], 
   avg(aptds('surface)) * 0.2,
   litAggr("Hello World")
).show().run()
```


## Joins

```scala mdoc:silent
case class CityPopulationInfo(name: String, population: Int)

val cityInfo = Seq(
  CityPopulationInfo("Paris", 2229621),
  CityPopulationInfo("Lyon", 500715),
  CityPopulationInfo("Nice", 343629)
)

val citiInfoTypedDS = TypedDataset.create(cityInfo)
```

Here is how to join the population information to the apartment's dataset:

```scala mdoc
val withCityInfo = aptTypedDs.joinInner(citiInfoTypedDS) { aptTypedDs('city) === citiInfoTypedDS('name) }

withCityInfo.show().run()
```

The joined TypedDataset has type `TypedDataset[(Apartment, CityPopulationInfo)]`.

We can then select which information we want to continue to work with:

```scala mdoc
case class AptPriceCity(city: String, aptPrice: Double, cityPopulation: Int)

withCityInfo.select(
   withCityInfo.colMany('_2, 'name), withCityInfo.colMany('_1, 'price), withCityInfo.colMany('_2, 'population)
).as[AptPriceCity].show().run
```

```scala mdoc:invisible
spark.stop()
```
