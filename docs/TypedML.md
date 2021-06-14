# Typed Spark ML

The `frameless-ml` module provides a strongly typed Spark ML API leveraging `TypedDataset`s. It introduces `TypedTransformer`s
and `TypedEstimator`s, the type-safe equivalents of Spark ML's `Transformer` and `Estimator`. 

A `TypedEstimator` fits models to data, i.e trains a ML model based on an input `TypedDataset`. 
A `TypedTransformer` transforms one `TypedDataset` into another, usually by appending column(s) to it.

By calling the `fit` method of a `TypedEstimator`, the `TypedEstimator` will train a ML model using the `TypedDataset` 
passed as input (representing the training data) and will return a `TypedTransformer` that represents the trained model. 
This `TypedTransformer`can then be used to make predictions on an input `TypedDataset` (representing the test data) 
using the `transform` method that will return a new `TypedDataset` with appended prediction column(s).

Both `TypedEstimator` and `TypedTransformer` check at compile-time the correctness of their inputs field names and types,
contrary to Spark ML API which only deals with DataFrames (the data structure with the lowest level of type-safety in Spark).

`frameless-ml` adds type-safety to Spark ML API but stays very close to it in terms of abstractions and API calls, so 
please check [Spark ML documentation](https://spark.apache.org/docs/2.2.0/ml-pipeline.html) for more details 
on `Transformer`s and `Estimator`s.

```scala mdoc:invisible:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```

## Example 1: predict a continuous value using a `TypedRandomForestRegressor`

In this example, we want to predict the sale price of a house depending on its square footage and the fact that the house
has a garden or not. We will use a `TypedRandomForestRegressor`.

### Training

As with the Spark ML API, we use a `TypedVectorAssembler` (the type-safe equivalent of `VectorAssembler`)
to compute feature vectors:

```scala mdoc:silent
import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector
```

```scala mdoc
case class HouseData(squareFeet: Double, hasGarden: Boolean, price: Double)

val trainingData = TypedDataset.create(Seq(
  HouseData(20, false, 100000),
  HouseData(50, false, 200000),
  HouseData(50, true, 250000),
  HouseData(100, true, 500000)
))

case class Features(squareFeet: Double, hasGarden: Boolean)
val assembler = TypedVectorAssembler[Features]

case class HouseDataWithFeatures(squareFeet: Double, hasGarden: Boolean, price: Double, features: Vector)
val trainingDataWithFeatures = assembler.transform(trainingData).as[HouseDataWithFeatures]
```

In the above code snippet, `.as[HouseDataWithFeatures]` is a `TypedDataset`'s type-safe cast
(see [TypedDataset: Feature Overview](https://typelevel.org/frameless/FeatureOverview.html)):

```scala mdoc:silent
case class WrongHouseFeatures(
  squareFeet: Double,
  hasGarden: Int, // hasGarden has wrong type
  price: Double,
  features: Vector
)
```

```scala mdoc:fail
assembler.transform(trainingData).as[WrongHouseFeatures]
```

Moreover, `TypedVectorAssembler[Features]` will compile only if `Features` contains exclusively fields of type Numeric or Boolean:

```scala mdoc:silent
case class WrongFeatures(squareFeet: Double, hasGarden: Boolean, city: String)
```

```scala mdoc:fail
TypedVectorAssembler[WrongFeatures]
```

The subsequent call `assembler.transform(trainingData)` compiles only if `trainingData` contains all fields (names and types)
of `Features`:

```scala mdoc
case class WrongHouseData(squareFeet: Double, price: Double) // hasGarden is missing
val wrongTrainingData = TypedDataset.create(Seq(WrongHouseData(20, 100000)))
```

```scala mdoc:fail
assembler.transform(wrongTrainingData)
```

Then, we train the model. To train a Random Forest, one needs to feed it with features (what we predict from) and
with a label (what we predict). In our example, `price` is the label, `features` are the features:

```scala mdoc
case class RFInputs(price: Double, features: Vector)
val rf = TypedRandomForestRegressor[RFInputs]

val model = rf.fit(trainingDataWithFeatures).run()
```

`TypedRandomForestRegressor[RFInputs]` compiles only if `RFInputs`
contains only one field of type Double (the label) and one field of type Vector (the features):

```scala mdoc:silent
case class WrongRFInputs(labelOfWrongType: String, features: Vector)
```

```scala mdoc:fail
TypedRandomForestRegressor[WrongRFInputs]
```

The subsequent `rf.fit(trainingDataWithFeatures)` call compiles only if `trainingDataWithFeatures` contains the same fields
(names and types) as RFInputs.

```scala mdoc
val wrongTrainingDataWithFeatures = TypedDataset.create(Seq(HouseData(20, false, 100000))) // features are missing
```

```scala mdoc:fail
rf.fit(wrongTrainingDataWithFeatures) 
```

### Prediction

We now want to predict `price` for `testData` using the previously trained model. Like the Spark ML API,
`testData` has a default value for `price` (`0` in our case) that will be ignored at prediction time. We reuse
our `assembler` to compute the feature vector of `testData`.

```scala mdoc
val testData = TypedDataset.create(Seq(HouseData(70, true, 0)))
val testDataWithFeatures = assembler.transform(testData).as[HouseDataWithFeatures]

case class HousePricePrediction(
  squareFeet: Double,
  hasGarden: Boolean,
  price: Double,
  features: Vector,
  predictedPrice: Double
)
val predictions = model.transform(testDataWithFeatures).as[HousePricePrediction]

predictions.select(predictions.col('predictedPrice)).collect.run()
```

`model.transform(testDataWithFeatures)` will only compile if `testDataWithFeatures` contains a field `price` of type Double
and a field `features` of type Vector:

```scala mdoc:fail
model.transform(testData)
```

```scala mdoc:invisible
spark.stop()
```

## Example 2: predict a categorical value using a `TypedRandomForestClassifier`

```scala mdoc:invisible:reset-object
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("Frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector
```

In this example, we want to predict in which city a house is located depending on its price and its square footage. We use a
`TypedRandomForestClassifier`.

### Training

As with the Spark ML API, we use a `TypedVectorAssembler` to compute feature vectors and a `TypedStringIndexer`
to index `city` values in order to be able to pass them to a `TypedRandomForestClassifier`
(which only accepts Double values as label):

```scala mdoc:silent
import frameless.ml.classification._
```

```scala mdoc
case class HouseData(squareFeet: Double, city: String, price: Double)

val trainingData = TypedDataset.create(Seq(
  HouseData(100, "lyon", 100000),
  HouseData(200, "lyon", 200000),
  HouseData(100, "san francisco", 500000),
  HouseData(150, "san francisco", 900000)
))

case class Features(price: Double, squareFeet: Double)
val vectorAssembler = TypedVectorAssembler[Features]

case class HouseDataWithFeatures(squareFeet: Double, city: String, price: Double, features: Vector)
val dataWithFeatures = vectorAssembler.transform(trainingData).as[HouseDataWithFeatures]

case class StringIndexerInput(city: String)
val indexer = TypedStringIndexer[StringIndexerInput]
indexer.estimator.setHandleInvalid("keep")
val indexerModel = indexer.fit(dataWithFeatures).run()

case class HouseDataWithFeaturesAndIndex(
  squareFeet: Double,
  city: String,
  price: Double,
  features: Vector,
  cityIndexed: Double
)
val indexedData = indexerModel.transform(dataWithFeatures).as[HouseDataWithFeaturesAndIndex]
```

Then, we train the model:

```scala mdoc
case class RFInputs(cityIndexed: Double, features: Vector)
val rf = TypedRandomForestClassifier[RFInputs]

val model = rf.fit(indexedData).run()
```

### Prediction

We now want to predict `city` for `testData` using the previously trained model. Like the Spark ML API,
`testData` has a default value for `city` (empty string in our case) that will be ignored at prediction time. We reuse
our `vectorAssembler` to compute the feature vector of `testData` and our `indexerModel` to index `city`.

```scala mdoc
val testData = TypedDataset.create(Seq(HouseData(120, "", 800000)))

val testDataWithFeatures = vectorAssembler.transform(testData).as[HouseDataWithFeatures]
val indexedTestData = indexerModel.transform(testDataWithFeatures).as[HouseDataWithFeaturesAndIndex]

case class HouseCityPredictionInputs(features: Vector, cityIndexed: Double)
val testInput = indexedTestData.project[HouseCityPredictionInputs]

case class HouseCityPredictionIndexed(
  features: Vector,
  cityIndexed: Double,
  rawPrediction: Vector,
  probability: Vector,
  predictedCityIndexed: Double
)
val indexedPredictions = model.transform(testInput).as[HouseCityPredictionIndexed]
```

Then, we use a `TypedIndexToString` to get back a String value from `predictedCityIndexed`. `TypedIndexToString` takes
as input the label array computed by our previous `indexerModel`:

```scala mdoc
case class IndexToStringInput(predictedCityIndexed: Double)
val indexToString = TypedIndexToString[IndexToStringInput](indexerModel.transformer.labels)

case class HouseCityPrediction(
  features: Vector,
  cityIndexed: Double,
  rawPrediction: Vector,
  probability: Vector,
  predictedCityIndexed: Double,
  predictedCity: String
)
val predictions = indexToString.transform(indexedPredictions).as[HouseCityPrediction]

predictions.select(predictions.col('predictedCity)).collect.run()
```

## List of currently implemented `TypedEstimator`s

* `TypedRandomForestClassifier`
* `TypedRandomForestRegressor`
* ... [your contribution here](https://github.com/typelevel/frameless/issues/215) ... :)

## List of currently implemented `TypedTransformer`s

* `TypedIndexToString`
* `TypedStringIndexer`
* `TypedVectorAssembler`
* ... [your contribution here](https://github.com/typelevel/frameless/issues/215) ... :)
 
## Using Vector and Matrix with `TypedDataset`

`frameless-ml` provides `TypedEncoder` instances for `org.apache.spark.ml.linalg.Vector` 
and `org.apache.spark.ml.linalg.Matrix`:

```scala mdoc:silent
import frameless._
import frameless.ml._
import org.apache.spark.ml.linalg._
```

```scala mdoc
val vector = Vectors.dense(1, 2, 3)
val vectorDs = TypedDataset.create(Seq("label" -> vector))

val matrix = Matrices.dense(2, 1, Array(1, 2))
val matrixDs = TypedDataset.create(Seq("label" -> matrix))
```

Under the hood, Vector and Matrix are encoded using `org.apache.spark.ml.linalg.VectorUDT` 
and `org.apache.spark.ml.linalg.MatrixUDT`. This is possible thanks to the implicit derivation 
from `org.apache.spark.sql.types.UserDefinedType[A]` to `TypedEncoder[A]` defined in `TypedEncoder` companion object.

```scala mdoc:invisible
spark.stop()
```
