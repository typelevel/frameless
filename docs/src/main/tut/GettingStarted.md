# Getting started
This tutorial introduces `TypedDataset`s through a small toy example.
The following imports are needed to make all code examples compile.
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
We are now ready to instantiate a `TypedDataset[Appartment]`:
```tut:book
val appartmentsTypedDS = TypedDataset.create(appartments)
```
We can also create it from an existing `Dataset`:
```tut:book
val appartmentsDS = spark.createDataset(appartments)
val appartmentsTypedDS = TypedDataset.create(appartmentsDS)
```
## Typesafe column referencing
This is how we select a particular column from a `TypedDataset`: 
```tut:book
val cities: TypedDataset[String] = appartmentsTypedDS.select(appartmentsTypedDS('city))
```
This is completely safe, for instance suppose we made a typo:
```tut:book:fail
appartmentsTypedDS.select(appartmentsTypedDS('citi))                                          ^
```
This gets caught at compile-time.
With `Dataset` the error appears at run-time.
```tut:book:fail
appartmentsDS.select('citi)
``` 
Let us now try to get the price by surface unit: 
```tut:book:fail
val priceBySurfaceUnit = appartmentsTypedDS.select(appartmentsTypedDS('price)/appartmentsTypedDS('surface))                                                                          ^
```
Argh! Looks like we can't divide a `TypedColumn` of `Double` by `Int`. 
Well, we can cast our `Int`s to `Double`s explicitely to proceed with the computation.
```tut:book
val priceBySurfaceUnit = appartmentsTypedDS.select(appartmentsTypedDS('price)/appartmentsTypedDS('surface).cast[Double])
priceBySurfaceUnit.collect.run()
``` 
Or perform the cast implicitely :
```tut:book
import frameless.implicits.widen._

val priceBySurfaceUnit = appartmentsTypedDS.select(appartmentsTypedDS('price)/appartmentsTypedDS('surface))
priceBySurfaceUnit.collect.run()
``` 
Looks like it worked, but that `cast` looks unsafe right? Actually it is safe.
Let's try to cast a `TypedColumn` of `String` to `Double`: 
```tut:book:fail
appartmentsTypedDS('city).cast[Double]                            ^
``` 
The compile-time error tells us that to perform the cast, an evidence (in the form of `CatalystCast[String, Double]`) must be available.
To check the list of available `CatalystCast` instances https://github.com/adelbertc/frameless/blob/566bde7/core/src/main/scala/frameless/CatalystCast.scala. 
## GroupBy and Aggregations
Let's suppose we wanted to retrieve the average appartment price in each city
```tut:book
val priceByCity = appartmentsTypedDS.groupBy(appartmentsTypedDS('city)).agg(avg(appartmentsTypedDS('price)))
priceByCity.collect().run()
``` 
Again if we try to aggregate a column that can't be aggregated, we get a compilation error
```tut:book:fail
appartmentsTypedDS.groupBy(appartmentsTypedDS('city)).agg(avg(appartmentsTypedDS('city)))                                                         ^
```
```tut:invisible
spark.stop()
``` 
