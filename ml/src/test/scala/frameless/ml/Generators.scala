package frameless.ml

import frameless.arbDouble
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  implicit val arbVector: Arbitrary[Vector] = Arbitrary {
    val genDenseVector = Gen.listOf(arbDouble.arbitrary).map(doubles => Vectors.dense(doubles.toArray))
    val genSparseVector = genDenseVector.map(_.toSparse)

    Gen.oneOf(genDenseVector, genSparseVector)
  }

  implicit val arbMatrix: Arbitrary[Matrix] = Arbitrary {
    Gen.sized { nbRows =>
      Gen.sized { nbCols =>
        Gen.listOfN(nbRows * nbCols, arbDouble.arbitrary)
          .map(values => Matrices.dense(nbRows, nbCols, values.toArray))
      }
    }
  }

}
