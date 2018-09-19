package frameless.ml.clustering

import frameless.ml.FramelessMlSuite
import frameless.ml.classification.TypedKMeans
import org.apache.spark.ml.linalg.Vector
import org.scalatest.MustMatchers
import frameless._
import frameless.ml._
import frameless.ml.feature._

class ClusteringIntegrationTests extends FramelessMlSuite with MustMatchers {

  test("predict field2 from field1 using a K-means clustering") {
    case class Data(field1: Double, field2: Int)

    // Training

    val trainingDataDs = TypedDataset.create(Seq.fill(5)(Data(10D, 0)) :+ Data(100D,0))

    case class Features(field1: Double)
    val vectorAssembler = TypedVectorAssembler[Features]

    case class DataWithFeatures(field1: Double, field2: Int, features: Vector)
    val dataWithFeatures = vectorAssembler.transform(trainingDataDs).as[DataWithFeatures]

    case class KMInputs(features: Vector)
    val km = TypedKMeans[KMInputs](2)

    val model = km.fit(dataWithFeatures).run()

    // Prediction
    val testSeq = Seq(
      Data(10D, 0),
      Data(100D, 1)
    )

    val testData = TypedDataset.create(testSeq)
    val testDataWithFeatures = vectorAssembler.transform(testData).as[DataWithFeatures]

    case class PredictionResult(field1: Double, field2: Int, features: Vector, predictedField2: Int)
    val predictionDs = model.transform(testDataWithFeatures).as[PredictionResult]

    val prediction = predictionDs.select(predictionDs.col[Int]('predictedField2)).collect.run().toList

    prediction mustEqual testSeq.map(_.field2)
  }
}
