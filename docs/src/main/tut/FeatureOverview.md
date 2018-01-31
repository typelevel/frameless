# TypedDataset: Feature Overview

This tutorial introduces `TypedDataset` using a simple example.
The following imports are needed to make all code examples compile.

```tut:silent
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

```tut:silent
case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
```

And few `Apartment` instances:

```tut:silent
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

```tut:book
val aptTypedDs = TypedDataset.create(apartments)
```

We can also create one from an existing Spark `Dataset`:

```tut:book
val aptDs = spark.createDataset(apartments)
val aptTypedDs = TypedDataset.create(aptDs)
```

Or use the Frameless syntax:

```tut:book
import frameless.syntax._

val aptTypedDs2 = aptDs.typed
```

## Typesafe column referencing
This is how we select a particular column from a `TypedDataset`:

```tut:book
val cities: TypedDataset[String] = aptTypedDs.select(aptTypedDs('city))
```

This is completely type-safe, for instance suppose we misspell `city` as `citi`:

```tut:book:fail
aptTypedDs.select(aptTypedDs('citi))
```

This gets raised at compile time, whereas with the standard `Dataset` API the error appears at runtime (enjoy the stack trace):

```tut:book:fail
aptDs.select('citi)
```

`select()` supports arbitrary column operations:

```tut:book
aptTypedDs.select(aptTypedDs('surface) * 10, aptTypedDs('surface) + 2).show().run()
```

Note that unlike the standard Spark API, where some operations are lazy and some are not, **all TypedDatasets operations are lazy.**
In the above example, `show()` is lazy. It requires to apply `run()` for the `show` job to materialize.
A more detailed explanation of `Job` is given [here](Job.md).

Next we compute the price by surface unit:

```tut:book:fail
val priceBySurfaceUnit = aptTypedDs.select(aptTypedDs('price) / aptTypedDs('surface))
```

As the error suggests, we can't divide a `TypedColumn` of `Double` by `Int.`
For safety, in Frameless only math operations between same types is allowed:

```tut:book
val priceBySurfaceUnit = aptTypedDs.select(aptTypedDs('price) / aptTypedDs('surface).cast[Double])
priceBySurfaceUnit.collect().run()
```

Looks like it worked, but that `cast` seems unsafe right? Actually it is safe.
Let's try to cast a `TypedColumn` of `String` to `Double`:

```tut:book:fail
aptTypedDs('city).cast[Double]
```

The compile-time error tells us that to perform the cast, an evidence
(in the form of `CatalystCast[String, Double]`) must be available.
Since casting from `String` to `Double` is not allowed, this results
in a compilation error.

Check [here](https://github.com/typelevel/frameless/blob/master/core/src/main/scala/frameless/CatalystCast.scala)
for the set of available `CatalystCast.`

## TypeSafe TypedDataset casting and projections

With `select()` the resulting TypedDataset is of type `TypedDataset[TupleN[...]]` (with N in `[1...10]`).
For example, if we select three columns with types `String`, `Int`, and `Boolean` the result will have type
`TypedDataset[(String, Int, Boolean)]`. To select more than ten columns use the `selectMany()` method.
Select has better IDE support than the macro based selectMany, so prefer `select()` for the general case.

We often want to give more expressive types to the result of our computations.
`as[T]` allows us to safely cast a `TypedDataset[U]` to another of type `TypedDataset[T]` as long
as the types in `U` and `T` align.

When the cast is valid the expression compiles:

```tut:book
case class UpdatedSurface(city: String, surface: Int)
val updated = aptTypedDs.select(aptTypedDs('city), aptTypedDs('surface) + 2).as[UpdatedSurface]
updated.show(2).run()
```

Next we try to cast a `(String, String)` to an `UpdatedSurface` (which has types `String`, `Int`).
The cast is not valid and the expression does not compile:

```tut:book:fail
aptTypedDs.select(aptTypedDs('city), aptTypedDs('city)).as[UpdatedSurface]
```

### Projections

We often want to work with a subset of the fields in a dataset.
Projections allows to easily select the fields we are interested
while preserving their initial name and types for extra safety.

Here is an example using the `TypedDataset[Apartment]` with an additional column:

```tut:book
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

```tut:book
case class CityInfo(city: String, ratio: Double)

val cityRatio = aptWithRatio.project[CityInfo]

cityRatio.show(2).run()
```

Suppose we only want to work with `price` and `ratio`:

```tut:book
case class PriceInfo(ratio: Double, price: Double)

val priceInfo = aptWithRatio.project[PriceInfo]

priceInfo.show(2).run()
```

We see that the order of the fields does not matter as long as the
names and the corresponding types agree. However, if we make a mistake in
any of the names and/or their types, then we get a compilation error.

Say we make a typo in a field name:

```tut:silent
case class PriceInfo2(ratio: Double, pricEE: Double)
```

```tut:book:fail
aptWithRatio.project[PriceInfo2]
```

Say we make a mistake in the corresponding type:

```tut:silent
case class PriceInfo3(ratio: Int, price: Double) // ratio should be Double
```

```tut:book:fail
aptWithRatio.project[PriceInfo3]
```

### Union uses the common fields

Lets create a projection of our original dataset with a subset of the fields.

```tut:silent
case class ApartmentShortInfo(city: String, price: Double, bedrooms: Int)

val aptTypedDs2: TypedDataset[ApartmentShortInfo] = aptTypedDs.project[ApartmentShortInfo]
```

The union of `aptTypedDs2` with `aptTypedDs` uses all the fields of the caller (`aptTypedDs2`)
and expects the other (`aptTypedDs`) dataset to include all those fields. If fields do not much
you will get a compilation error. 

```tut:book
aptTypedDs2.union(aptTypedDs).show().run
```

The other way around will not compile, since `aptTypedDs2` has only a subset of the fields. 

```tut:book:fail
aptTypedDs.union(aptTypedDs2).show().run
```

Finally, as with `project`, `union` will align fields that have same names/types,
so fields do not have to be in the same order. 


### Drop/Replace/Add fields to a TypedDataset

`dropTupled()` drops a single column and results in a tuple-based schema.

```tut:book
aptTypedDs2.dropTupled('price): TypedDataset[(String,Int)]
```

To drop columns and use a specific schema backed up with a case class use `drop()`.

```tut:book
case class CityBeds(city: String, bedrooms: Int)
val cityBeds: TypedDataset[CityBeds] = aptTypedDs2.drop[CityBeds] 
```

Often, you just want to replace a single column with a modified value.
 
```tut:book
val inflation = aptTypedDs2.withColumnReplaced('price, aptTypedDs2('price) * 2)
 
inflation.show(2).run()
```

Or use a literal instead.

```tut:book
import frameless.functions.lit
aptTypedDs2.withColumnReplaced('price, lit(0.001)) 
```

Adding a column using `withColumnTupled()` results in a tupled-based schema.

```tut:book
aptTypedDs2.withColumnTupled(lit(Array("a","b","c"))).show(2).run()
```

Similarly, `withColumn()` adds a column expecting a schema.

```tut:book
case class CityBedsOther(city: String, bedrooms: Int, other: List[String])

cityBeds.
   withColumn[CityBedsOther](lit(List("a","b","c"))).
   show(1).run()
```

Finally, we can conditionally change a column using the `when/otherwise` operation. 

```tut:book
import frameless.functions.nonAggregate.when
aptTypedDs2.withColumnTupled(
   when(aptTypedDs2('city) === "Paris", aptTypedDs2('price)).
   when(aptTypedDs2('city) === "Lyon", lit(1.1)).
   otherwise(lit(0.0))).show(8).run()
```


## User Defined Functions

Frameless supports lifting any Scala function (up to five arguments) to the
context of a particular `TypedDataset`:

```tut:book
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
```tut:book
val priceByCity = aptTypedDs.groupBy(aptTypedDs('city)).agg(avg(aptTypedDs('price)))
priceByCity.collect().run()
```
Again if we try to aggregate a column that can't be aggregated, we get a compilation error
```tut:book:fail
aptTypedDs.groupBy(aptTypedDs('city)).agg(avg(aptTypedDs('city)))
```

Next, we combine `select` and `groupBy` to calculate the average price/surface ratio per city:

```tut:book
val aptds = aptTypedDs // For shorter expressions

val cityPriceRatio =  aptds.select(aptds('city), aptds('price) / aptds('surface).cast[Double])

cityPriceRatio.groupBy(cityPriceRatio('_1)).agg(avg(cityPriceRatio('_2))).show().run()
```

We can also use `pivot` to further group data on a secondary column.
For example, we can compare the average price across cities by number of bedrooms.

```tut:book
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

```tut:book
bedroomStats.collect().run().foreach(println)
```

#### Working with Optional fields

Optional fields can be converted to non-optional using `getOrElse()`. 

```tut:book
val sampleStats = bedroomStats.select(
   bedroomStats('AvgPriceBeds2).getOrElse(0.0),
   bedroomStats('AvgPriceBeds3).getOrElse(0.0))

sampleStats.show().run()   
``` 


### Entire TypedDataset Aggregation

We often want to aggregate the entire `TypedDataset` and skip the `groupBy()` clause.
In Frameless you can do this using the `agg()` operator directly on the `TypedDataset`.
In the following example, we compute the average price, the average surface,
the minimum surface, and the set of cities for the entire dataset.

```tut:book
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

```tut:book
import frameless.functions._
aptds.agg(
   avg(aptds('price)) * min(aptds('surface)).cast[Double], 
   avg(aptds('surface)) * 0.2,
   litAggr("Hello World")
).show().run()
```


## Joins

```tut:silent
case class CityPopulationInfo(name: String, population: Int)

val cityInfo = Seq(
  CityPopulationInfo("Paris", 2229621),
  CityPopulationInfo("Lyon", 500715),
  CityPopulationInfo("Nice", 343629)
)

val citiInfoTypedDS = TypedDataset.create(cityInfo)
```

Here is how to join the population information to the apartment's dataset:

```tut:book
val withCityInfo = aptTypedDs.joinInner(citiInfoTypedDS) { aptTypedDs('city) === citiInfoTypedDS('name) }

withCityInfo.show().run()
```

The joined TypedDataset has type `TypedDataset[(Apartment, CityPopulationInfo)]`.

We can then select which information we want to continue to work with:

```tut:book
case class AptPriceCity(city: String, aptPrice: Double, cityPopulation: Int)

withCityInfo.select(
   withCityInfo.colMany('_2, 'name), withCityInfo.colMany('_1, 'price), withCityInfo.colMany('_2, 'population)
).as[AptPriceCity].show().run
```

```tut:invisible
spark.stop()
```
