# TypedDataset: Feature Overview

This tutorial introduces `TypedDataset`s through a small toy example.
The following imports are needed to make all code examples compile.

```tut:silent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
import frameless.TypedDataset

val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
implicit val sqlContext = spark.sqlContext
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```

## Creating TypedDataset instances

We start by defining a simple case class that will be the basis of our examples.

```tut:silent
case class Apartment(city: String, surface: Int, price: Double)
```

And let's define a few `Apartment` instances:

```tut:silent
val apartments = Seq(
  Apartment("Paris", 50, 300000.0),
  Apartment("Paris", 100, 450000.0),
  Apartment("Paris", 25, 250000.0),
  Apartment("Lyon", 83, 200000.0),
  Apartment("Lyon", 45, 133000.0),
  Apartment("Nice", 74, 325000.0)
)
```

We are now ready to instantiate a `TypedDataset[Apartment]`:

```tut:book
val apartmentsTypedDS = TypedDataset.create(apartments)
```

We can also create it from an existing `Dataset`:

```tut:book
val apartmentsDS = spark.createDataset(apartments)
val apartmentsTypedDS = TypedDataset.create(apartmentsDS)
```

Or use the frameless syntax:

```tut:book
import frameless.syntax._

val apartmentsTypedDS2 = spark.createDataset(apartments).typed
```

## Typesafe column referencing
This is how we select a particular column from a `TypedDataset`:

```tut:book
val cities: TypedDataset[String] = apartmentsTypedDS.select(apartmentsTypedDS(_.city))
```

This is completely safe, for instance suppose we misspell `city`:

```tut:book:fail
apartmentsTypedDS.select(apartmentsTypedDS(_.citi))
```

This gets caught at compile-time, whereas with traditional Spark `Dataset` the error appears at run-time.

```tut:book:fail
apartmentsDS.select('citi)
```

`select()` supports arbitrary column operations:

```tut:book
apartmentsTypedDS.select(apartmentsTypedDS(_.surface) * 10, apartmentsTypedDS(_.surface) + 2).show().run()
```

*Note that unlike the standard Spark api, here `show()` is lazy. It requires to apply `run()` for the
 `show` job to materialize.*


Let us now try to compute the price by surface unit:

```tut:book:fail
val priceBySurfaceUnit = apartmentsTypedDS.select(apartmentsTypedDS(_.price)/apartmentsTypedDS(_.surface))                                                                          ^
```

Argh! Looks like we can't divide a `TypedColumn` of `Double` by `Int`.
Well, we can cast our `Int`s to `Double`s explicitly to proceed with the computation.

```tut:book
val priceBySurfaceUnit = apartmentsTypedDS.select(apartmentsTypedDS(_.price)/apartmentsTypedDS(_.surface).cast[Double])
priceBySurfaceUnit.collect().run()
```

Alternatively, we can perform the cast implicitly:

```tut:book
import frameless.implicits.widen._

val priceBySurfaceUnit = apartmentsTypedDS.select(apartmentsTypedDS(_.price)/apartmentsTypedDS(_.surface))
priceBySurfaceUnit.collect.run()
```

Looks like it worked, but that `cast` looks unsafe right? Actually it is safe.
Let's try to cast a `TypedColumn` of `String` to `Double`:

```tut:book:fail
apartmentsTypedDS(_.city).cast[Double]
```

The compile-time error tells us that to perform the cast, an evidence (in the form of `CatalystCast[String, Double]`) must be available.

Check [here](https://github.com/adelbertc/frameless/blob/master/core/src/main/scala/frameless/CatalystCast.scala) for the set of available `CatalystCast`.

## TypeSafe TypedDataset casting and projections

With `select()` the resulting TypedDataset is of type `TypedDataset[TupleN[...]]` (with N in `[1...10]`).
For example, if we select three columns with types `String`, `Int`, and `Boolean` the result will have type
`TypedDataset[(String, Int, Boolean)]`.
We often want to give more expressive types to the result of our computations.

`as[T]` allows us to safely cast a `TypedDataset[U]` to another of type `TypedDataset[T]` as long
as the types in `U` and `T` align.

The cast is valid and the expression compiles:

```tut:book
case class UpdatedSurface(city: String, surface: Int)
val updated = apartmentsTypedDS.select(apartmentsTypedDS(_.city), apartmentsTypedDS(_.surface) + 2).as[UpdatedSurface]
updated.show(2).run()
```

Next we try to cast a `(String, String)` to an `UpdatedSurface` (which has types `String`, `Int`).
The cast is not valid and the expression does not compile:

```tut:book:fail
apartmentsTypedDS.select(apartmentsTypedDS(_.city), apartmentsTypedDS(_.city)).as[UpdatedSurface]
```

### Projections

We often want to work with a subset of the fields in a dataset.
Projections allows to easily select the fields we are interested
while preserving their initial name and types for extra safety.

Here is an example using the `TypedDataset[Apartment]` with an additional column:

```tut:book
import frameless.implicits.widen._

val aptds = apartmentsTypedDS // For shorter expressions

case class ApartmentDetails(city: String, price: Double, surface: Int, ratio: Double)
val aptWithRatio = aptds.select(aptds(_.city), aptds(_.price), aptds(_.surface), aptds(_.price) / aptds(_.surface)).as[ApartmentDetails]
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

We see here that the order of the fields doesn't matter as long as the
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

## User Defined Functions

Frameless supports lifting any Scala function (up to five arguments) to the
context of a particular `TypedDataset`:

```tut:book
// The function we want to use as UDF
val priceModifier =
    (name: String, price:Double) => if(name == "Paris") price * 2.0 else price

val udf = apartmentsTypedDS.makeUDF(priceModifier)

val aptds = apartmentsTypedDS // For shorter expressions

val adjustedPrice = aptds.select(aptds(_.city), udf(aptds(_.city), aptds(_.price)))

adjustedPrice.show().run()
```

## GroupBy and Aggregations
Let's suppose we wanted to retrieve the average apartment price in each city
```tut:book
val priceByCity = apartmentsTypedDS.groupBy(apartmentsTypedDS(_.city)).agg(avg(apartmentsTypedDS(_.price)))
priceByCity.collect().run()
```
Again if we try to aggregate a column that can't be aggregated, we get a compilation error
```tut:book:fail
apartmentsTypedDS.groupBy(apartmentsTypedDS(_.city)).agg(avg(apartmentsTypedDS(_.city)))                                                         ^
```

Next, we combine `select` and `groupBy` to calculate the average price/surface ratio per city:

```tut:book
val aptds = apartmentsTypedDS // For shorter expressions

val cityPriceRatio =  aptds.select(aptds(_.city), aptds(_.price) / aptds(_.surface))

cityPriceRatio.groupBy(cityPriceRatio(_._1)).agg(avg(cityPriceRatio(_._2))).show().run()
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

Here is how to join the population information to the apartment's dataset.

```tut:book
val withCityInfo = apartmentsTypedDS.join(citiInfoTypedDS, apartmentsTypedDS(_.city), citiInfoTypedDS(_.name))

withCityInfo.show().run()
```

The joined TypedDataset has type `TypedDataset[(Apartment, CityPopulationInfo)]`.

We can then select which information we want to continue to work with:

```tut:book
case class AptPriceCity(city: String, aptPrice: Double, cityPopulation: Int)

withCityInfo.select(
   withCityInfo(_._2.name), withCityInfo(_._1.price), withCityInfo(_._2.population)
).as[AptPriceCity].show().run
```

```tut:invisible
spark.stop()
```
