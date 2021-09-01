package frameless
package ml
package classification

import org.apache.spark.ml.linalg.Vector

import frameless.ml.feature.{TypedIndexToString, TypedStringIndexer, TypedVectorAssembler}

import org.scalatest.matchers.must.Matchers

class ClassificationIntegrationTests extends FramelessMlSuite with Matchers {

  test("predict field3 from field1 and field2 using a RandomForestClassifier") {
    case class Data(field1: Double, field2: Int, field3: String)

    // Training

    val trainingDataDs = TypedDataset.create(Seq.fill(10)(Data(0d, 10, "foo")))

    case class Features(field1: Double, field2: Int)
    val vectorAssembler = TypedVectorAssembler[Features]

    case class DataWithFeatures(field1: Double, field2: Int, field3: String, features: Vector)
    val dataWithFeatures = vectorAssembler.transform(trainingDataDs).as[DataWithFeatures]

    case class StringIndexerInput(field3: String)
    val indexer = TypedStringIndexer[StringIndexerInput]
    val indexerModel = indexer.fit(dataWithFeatures).run()

    case class IndexedDataWithFeatures(
        field1: Double,
        field2: Int,
        field3: String,
        features: Vector,
        indexedField3: Double)
    val indexedData = indexerModel.transform(dataWithFeatures).as[IndexedDataWithFeatures]

    case class RFInputs(indexedField3: Double, features: Vector)
    val rf = TypedRandomForestClassifier[RFInputs]

    val model = rf.fit(indexedData).run()

    // Prediction

    val testData = TypedDataset.create(
      Seq(
        Data(0d, 10, "foo")
      ))
    val testDataWithFeatures = vectorAssembler.transform(testData).as[DataWithFeatures]
    val indexedTestData =
      indexerModel.transform(testDataWithFeatures).as[IndexedDataWithFeatures]

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

    case class IndexToStringInput(predictedField3Indexed: Double)
    val indexToString =
      TypedIndexToString[IndexToStringInput](indexerModel.transformer.labelsArray.flatten)

    case class PredictionResult(
        features: Vector,
        indexedField3: Double,
        rawPrediction: Vector,
        probability: Vector,
        predictedField3Indexed: Double,
        predictedField3: String
    )
    val stringPredictionDs = indexToString.transform(predictionDs).as[PredictionResult]

    val prediction =
      stringPredictionDs.select(stringPredictionDs.col('predictedField3)).collect.run().toList

    prediction mustEqual List("foo")
  }

}
