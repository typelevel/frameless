import frameless.TypedEncoder.UDT
import org.scalacheck.{Arbitrary, Gen}
import org.apache.spark.ml.linalg.{Matrices, Matrix, SQLDataTypes, Vector => MLVector, Vectors => MLVectors}

package object frameless {
  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary {
    for {
      x <- Gen.chooseNum(-1000, 1000)
      y <- Gen.chooseNum(0, 1000000)
    } yield BigDecimal(s"$x.$y")
  }

  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbDouble: Arbitrary[Double] = Arbitrary {
    arbBigDecimal.arbitrary.map(_.toDouble)
  }

  implicit val arbSqlDate = Arbitrary {
    Arbitrary.arbitrary[Int].map(SQLDate)
  }

  implicit val arbSqlTimestamp = Arbitrary {
    Arbitrary.arbitrary[Long].map(SQLTimestamp)
  }

  implicit def arbTuple1[A: Arbitrary] = Arbitrary {
    Arbitrary.arbitrary[A].map(Tuple1(_))
  }

  // see issue with scalacheck non serializable Vector: https://github.com/rickynils/scalacheck/issues/315
  implicit def arbVector[A](implicit A: Arbitrary[A]): Arbitrary[Vector[A]] =
    Arbitrary(Gen.listOf(A.arbitrary).map(_.toVector))

  implicit val arbMLVector: Arbitrary[MLVector] = Arbitrary {
    val genDenseVector = Gen.listOf(arbDouble.arbitrary).map(doubles => MLVectors.dense(doubles.toArray))
    val genSparseVector = genDenseVector.map(_.toSparse)

    Gen.oneOf(genDenseVector, genSparseVector)
  }

  implicit val mLVectorUDT: UDT[MLVector] = SQLDataTypes.VectorType.asInstanceOf[UDT[MLVector]]

  implicit val arbMatrix: Arbitrary[Matrix] = Arbitrary {
    Gen.sized { nbRows =>
      Gen.sized { nbCols =>
        Gen.listOfN(nbRows * nbCols, arbDouble.arbitrary)
          .map(values => Matrices.dense(nbRows, nbCols, values.toArray))
      }
    }
  }

  implicit val matrixUDT: UDT[Matrix] = SQLDataTypes.MatrixType.asInstanceOf[UDT[Matrix]]

}
