package frameless
package ml

import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}

import frameless.ml.params.linears.{LossStrategy, Solver}
import frameless.ml.params.trees.FeatureSubsetStrategy

import org.scalacheck.{Arbitrary, Gen}

object Generators {

  implicit val arbVector: Arbitrary[Vector] = Arbitrary {
    val genDenseVector =
      Gen.listOf(arbDouble.arbitrary).map(doubles => Vectors.dense(doubles.toArray))
    val genSparseVector = genDenseVector.map(_.toSparse)

    Gen.oneOf(genDenseVector, genSparseVector)
  }

  implicit val arbMatrix: Arbitrary[Matrix] = Arbitrary {
    Gen.sized { size =>
      for {
        nbRows <- Gen.choose(0, size)
        nbCols <- Gen.choose(1, size)
        matrix <- {
          Gen
            .listOfN(nbRows * nbCols, arbDouble.arbitrary)
            .map(values => Matrices.dense(nbRows, nbCols, values.toArray))
        }
      } yield matrix
    }
  }

  implicit val arbTreesFeaturesSubsetStrategy: Arbitrary[FeatureSubsetStrategy] = Arbitrary {
    val genRatio = Gen.choose(0d, 1d).suchThat(_ > 0d).map(FeatureSubsetStrategy.Ratio)
    val genNumberOfFeatures =
      Gen.choose(1, Int.MaxValue).map(FeatureSubsetStrategy.NumberOfFeatures)

    Gen.oneOf(
      Gen.const(FeatureSubsetStrategy.All),
      Gen.const(FeatureSubsetStrategy.All),
      Gen.const(FeatureSubsetStrategy.Log2),
      Gen.const(FeatureSubsetStrategy.OneThird),
      Gen.const(FeatureSubsetStrategy.Sqrt),
      genRatio,
      genNumberOfFeatures
    )
  }

  implicit val arbLossStrategy: Arbitrary[LossStrategy] = Arbitrary {
    Gen.const(LossStrategy.SquaredError)
  }

  implicit val arbSolver: Arbitrary[Solver] = Arbitrary {
    Gen.oneOf(
      Gen.const(Solver.LBFGS),
      Gen.const(Solver.Auto),
      Gen.const(Solver.Normal)
    )
  }

}
