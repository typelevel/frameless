package frameless
package ml

import org.scalacheck.Prop._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.regression.DecisionTreeRegressor
import Generators._
import scala.util.Random

class TypedEncoderInstancesTests extends FramelessMlSuite {

  test("Vector encoding is injective using collect()") {
    val prop = forAll { vector: Vector =>
      TypedDataset.create(Seq(vector)).collect().run() == Seq(vector)
    }
    check(prop)
  }

  test("Matrix encoding is injective using collect()") {
    val prop = forAll { matrix: Matrix =>
      TypedDataset.create(Seq(matrix)).collect().run() == Seq(matrix)
    }
    check(prop)
  }

  test(
    "Vector is encoded as VectorUDT and thus can be run in a Spark ML model"
  ) {
    case class Input(features: Vector, label: Double)

    val prop = forAll { trainingData: Matrix =>
      (trainingData.numRows >= 1) ==> {
        val inputs =
          trainingData.rowIter.toVector.map(vector => Input(vector, 0D))
        val inputsDS = TypedDataset.create(inputs)

        val model = new DecisionTreeRegressor()

        // this line would throw a runtime exception if Vector was not encoded as VectorUDT
        val trainedModel = model.fit(inputsDS.dataset)

        val randomInput = inputs(Random.nextInt(inputs.length))
        val randomInputDS = TypedDataset.create(Seq(randomInput))

        val prediction = trainedModel
          .transform(randomInputDS.dataset)
          .select("prediction")
          .head()
          .getAs[Double](0)

        prediction == 0D
      }

    }

    check(prop, MinSize(1))
  }

}
