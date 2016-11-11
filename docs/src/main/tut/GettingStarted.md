# Getting started
We are going to manipulate some `TypedDataset`s through a small toy example.
Before starting the tutorial, let's import/define a couple of things.
```tut:book
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
```tut:book
case class Appartment(city: String, surface: Int, price: Double)
``` 
And let's define a few `Appartment` instances:
```tut:book
val appartments = Seq(
    Appartment("Paris", 50, 300000.0),
    Appartment("Paris", 100, 450000.0),
    Appartment("Paris", 25, 250000.0),
    Appartment("Lyon", 83, 200000.0),
    Appartment("Lyon", 45, 133000.0),
    Appartment("Nice", 74, 325000.0)
    )
```
We can create our `TypedDataset` directly :
```tut:book
val appartmentsTypedDS = TypedDataset.create(appartments)
```
We can also create it from an existing `Dataset` :
```tut:book
val appartmentsDS = spark.createDataset(appartments)
val appartmentsTypedDS = TypedDataset.create(appartmentsDS)
```
## Typesafe column referencing
Selecting a column from our `TypedDataset` is done in this manner : 
```tut:book
val cities: TypedDataset[String] = appartmentsTypedDS.select(appartmentsTypedDS('city))
```
This is completely safe, for instance suppose we made a typo :
```tut:book:fail
appartmentsTypedDS.select(appartmentsTypedDS('citi))                                          ^
```
This gets caught at compile time.
With `Dataset` the error would have been caught at runtime.
```tut:book:fail
appartmentsDS.select('citi)
``` 
Let us now try to get the price by surface unit : 
```tut:book:fail
val priceBySurfaceUnit = appartmentsTypedDS.select(appartmentsTypedDS('price)/appartmentsTypedDS('surface))                                                                          ^
```
Argh! Looks like we can't divide a `TypedColumn` of `Double` by `Int`. 
Well, we can cast our `Int`s to `Double`s explicitely to proceed with the computation.
```tut:book
val priceBySurfaceUnit = appartmentsTypedDS.select(appartmentsTypedDS('price)/appartmentsTypedDS('surface).cast[Double])
priceBySurfaceUnit.collect.run()
``` 
Looks like it worked, but that `cast` looks unsafe right ? Actually it is safe.
Let's try to cast `TypedColumn` of `String` to `Double` : 
```tut:book:fail
appartmentsTypedDS('city).cast[Double]                            ^
``` 
The compile-time error tells us that to perform the cast, an evidence (in the form of `CatalystCast[String,Double]`) must be available.
## GroupBy and Aggregations
Let's suppose we wanted to retrieve the average appartment price in each city
```tut:book
val priceByCity = appartmentsTypedDS.groupBy(appartmentsTypedDS('city)).agg(avg(appartmentsTypedDS('price)))
priceByCity.collect().run()
``` 
Again if we try to aggregate a column that can't be aggregated, we get a compile time error
```tut:book:fail
appartmentsTypedDS.groupBy(appartmentsTypedDS('city)).agg(avg(appartmentsTypedDS('city)))                                                         ^
```
