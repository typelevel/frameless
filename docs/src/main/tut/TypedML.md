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

`frameless-ml` adds type-safety to Spark ML API but stays very close to it in terms of abstractions and code flow, so 
please check [Spark ML documentation](https://spark.apache.org/docs/2.2.0/ml-pipeline.html) for more details 
on `Transformer`s and `Estimator`s.

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
implicit val sqlContext = spark.sqlContext
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```

## Example 1: predict a continuous value using a `TypedRandomForestRegressor`

In this example, we want to predict `field3` of type Double from `field1` and `field2` using 
a `TypedRandomForestRegressor`.

### Training

As with Spark ML API, we use a `TypedVectorAssembler` (the type-safe equivalent of `VectorAssembler`) to compute feature vectors:

```tut:silent
import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector
```

```tut:book
case class Data(field1: Double, field2: Int, field3: Double)

val trainingData = TypedDataset.create(
  Seq.fill(10)(Data(0D, 10, 0D))
)

case class Features(field1: Double, field2: Int)
val assembler = TypedVectorAssembler[Features]

case class DataWithFeatures(field1: Double, field2: Int, field3: Double, features: Vector)
val trainingDataWithFeatures = assembler.transform(trainingData).as[DataWithFeatures]
```

Then, we train the model:

```tut:book
case class RFInputs(field3: Double, features: Vector)
val rf = TypedRandomForestRegressor[RFInputs]

val model = rf.fit(trainingDataWithFeatures).run()
```

As an example of added type-safety, `TypedRandomForestRegressor[RFInputs]` will compile only if the given `RFInputs` case class 
contains only one field of type Double (the label) and one field of type Vector (the features):

```tut:silent
case class WrongRFInputs(labelOfWrongType: String, features: Vector)
```

```tut:book:fail
TypedRandomForestRegressor[WrongRFInputs]
```

The subsequent `rf.fit(trainingDataWithFeatures)` call will compile only if `trainingDataWithFeatures` contains the same fields 
(names and types) as RFInputs.

```tut:book
val wrongTrainingDataWithFeatures = TypedDataset.create(Seq(Data(0D, 1, 0D))) // features are missing
```

```tut:book:fail
rf.fit(wrongTrainingDataWithFeatures) 
```

For new-comers to frameless, please note that `typedDataset.as[...]` is a type-safe cast, 
see [TypedDataset: Feature Overview](https://typelevel.org/frameless/FeatureOverview.html):

```tut:silent
case class WrongTrainingDataWithFeatures(
  field1: Double, 
  field2: String, // field 2 has wrong type 
  field3: Double, 
  features: Vector
) 
```

```tut:book:fail
assembler.transform(trainingData).as[WrongTrainingDataWithFeatures]
```

### Prediction

We now want to predict `field3` for `testData` using the previously trained model. Please note that, like Spark ML API,
`testData` has a default value for `field3` (`0D` in our case) that will be ignored at prediction time. We reuse 
our `assembler` to compute the feature vector of `testData`.

```tut:book
val testData = TypedDataset.create(Seq(Data(0D, 10, 0D)))
val testDataWithFeatures = assembler.transform(testData).as[DataWithFeatures]

case class PredictionResult(
  field1: Double, 
  field2: Int, 
  field3: Double, 
  features: Vector, 
  predictedField3: Double
)
val results = model.transform(testDataWithFeatures).as[PredictionResult]

val predictions = results.select(results.col('predictedField3)).collect.run()

predictions == Seq(0D)
```

## Example 2: predict a categorical value using a `TypedRandomForestClassifier`

In this example, we want to predict `field3` of type String from `field1` and `field2` using a `TypedRandomForestClassifier`. 

### Training

As with Spark ML API, we use a `TypedVectorAssembler` to compute feature vectors and a `TypedStringIndexer` 
to index `field3` values in order to be able to pass them to a `TypedRandomForestClassifier` 
(which only accepts indexed Double values as label):

```tut:silent
import frameless.ml.classification._
```

```tut:book
case class Data(field1: Double, field2: Int, field3: String)

val trainingDataDs = TypedDataset.create(
  Seq.fill(10)(Data(0D, 10, "foo"))
)

case class Features(field1: Double, field2: Int)
val vectorAssembler = TypedVectorAssembler[Features]

case class DataWithFeatures(field1: Double, field2: Int, field3: String, features: Vector)
val dataWithFeatures = vectorAssembler.transform(trainingDataDs).as[DataWithFeatures]

case class StringIndexerInput(field3: String)
val indexer = TypedStringIndexer[StringIndexerInput]
val indexerModel = indexer.fit(dataWithFeatures).run()

case class IndexedDataWithFeatures(field1: Double, field2: Int, field3: String, features: Vector, indexedField3: Double)
val indexedData = indexerModel.transform(dataWithFeatures).as[IndexedDataWithFeatures]
```

Then, we train the model:

```tut:book
case class RFInputs(indexedField3: Double, features: Vector)
val rf = TypedRandomForestClassifier[RFInputs]

val model = rf.fit(indexedData).run()
```

### Prediction

We now want to predict `field3` for `testData` using the previously trained model. Please note that, like Spark ML API,
`testData` has a default value for `field3` (empty String in our case) that will be ignored at prediction time. We reuse 
our `vectorAssembler` to compute the feature vector of `testData` and our `indexerModel` to index `field3`.

```tut:book
val testData = TypedDataset.create(Seq(
  Data(0D, 10, "")
))
val testDataWithFeatures = vectorAssembler.transform(testData).as[DataWithFeatures]
val indexedTestData = indexerModel.transform(testDataWithFeatures).as[IndexedDataWithFeatures]

case class PredictionInputs(features: Vector, indexedField3: Double)
val testInput = indexedTestData.project[PredictionInputs]

case class PredictionResultIndexed(
  features: Vector,
  indexedField3: Double,
  rawPrediction: Vector,
  probability: Vector,
  predictedField3Indexed: Double
)
val predictionDs = model.transform(testInput).as[PredictionResultIndexed]
```

Then, we use a `TypedIndexToString` to get back a String value from `predictedField3`. `TypedIndexToString` takes
as input the label array computed by our previous `indexerModel`:

```tut:book
case class IndexToStringInput(predictedField3Indexed: Double)
val indexToString = TypedIndexToString[IndexToStringInput](indexerModel.transformer.labels)

case class PredictionResult(
  features: Vector,
  indexedField3: Double,
  rawPrediction: Vector,
  probability: Vector,
  predictedField3Indexed: Double,
  predictedField3: String
)
val stringPredictionDs = indexToString.transform(predictionDs).as[PredictionResult]

val prediction = stringPredictionDs.select(stringPredictionDs.col('predictedField3)).collect.run()

prediction == Seq("foo")
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

```tut:silent
import frameless.ml._
import org.apache.spark.ml.linalg._
```

```tut:book
val vector = Vectors.dense(1, 2, 3)
val vectorDs = TypedDataset.create(Seq("label" -> vector))

val matrix = Matrices.dense(2, 1, Array(1, 2))
val matrixDs = TypedDataset.create(Seq("label" -> matrix))
```

Under the hood, Vector and Matrix are encoded using `org.apache.spark.ml.linalg.VectorUDT` 
and `org.apache.spark.ml.linalg.MatrixUDT`. This is possible thanks to the implicit derivation 
from `org.apache.spark.sql.types.UserDefinedType[A]` to `TypedEncoder[A]` defined in `TypedEncoder` companion object.

```tut:invisible
spark.stop()
```
