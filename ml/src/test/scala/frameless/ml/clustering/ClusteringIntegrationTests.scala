package frameless
package ml
package clustering

import frameless.ml.FramelessMlSuite
import frameless.ml.classification.{TypedBisectingKMeans, TypedKMeans}
import org.apache.spark.ml.linalg.Vector
import org.scalatest.MustMatchers
import frameless._
import frameless.ml._
import frameless.ml.feature._

class ClusteringIntegrationTests extends FramelessMlSuite with MustMatchers {

  test("predict field2 from field1 using a K-means clustering") {
    // Training
    val trainingDataDs = TypedDataset.create(Seq.fill(5)(X2(10D, 0)) :+ X2(100D,0))

    val vectorAssembler = TypedVectorAssembler[X1[Double]]

    val dataWithFeatures = vectorAssembler.transform(trainingDataDs).as[X3[Double,Int,Vector]]

    case class Input(c: Vector)
    val km = TypedKMeans[Input].setK(2)

    val model = km.fit(dataWithFeatures).run()

    // Prediction
    val testSeq = Seq(
      X2(10D, 0),
      X2(100D, 1)
    )

    val testData = TypedDataset.create(testSeq)
    val testDataWithFeatures = vectorAssembler.transform(testData).as[X3[Double,Int,Vector]]

    val predictionDs = model.transform(testDataWithFeatures).as[X4[Double,Int,Vector,Int]]

    val prediction = predictionDs.select(predictionDs.col[Int]('d)).collect.run().toList

    prediction mustEqual testSeq.map(_.b)
  }

  test("predict field2 from field1 using a bisecting K-means clustering") {
    // Training
    val trainingDataDs = TypedDataset.create(Seq.fill(5)(X2(10D, 0)) :+ X2(100D,0))

    val vectorAssembler = TypedVectorAssembler[X1[Double]]

    val dataWithFeatures = vectorAssembler.transform(trainingDataDs).as[X3[Double, Int, Vector]]

    case class Inputs(c: Vector)
    val bkm = TypedBisectingKMeans[Inputs]().setK(2)

    val model = bkm.fit(dataWithFeatures).run()

    // Prediction
    val testSeq = Seq(
      X2(10D, 0),
      X2(100D, 1)
    )

    val testData = TypedDataset.create(testSeq)
    val testDataWithFeatures = vectorAssembler.transform(testData).as[X3[Double, Int, Vector]]

    val predictionDs = model.transform(testDataWithFeatures).as[X4[Double,Int,Vector,Int]]

    val prediction = predictionDs.select(predictionDs.col[Int]('d)).collect.run().toList

    prediction mustEqual testSeq.map(_.b)
  }

}
