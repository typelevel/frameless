package frameless
package ml
package feature

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.apache.spark.ml.linalg._
import shapeless.test.illTyped

class TypedVectorAssemblerTests extends FramelessMlSuite {

  test(".transform() returns a correct TypedTransformer") {
    def prop[A: TypedEncoder: Arbitrary] = forAll { x5: X5[Int, Long, Double, Boolean, A] =>
      val assembler = TypedVectorAssembler[X4[Int, Long, Double, Boolean]]
      val ds = TypedDataset.create(Seq(x5))
      val ds2 = assembler.transform(ds).run().as[X6[Int, Long, Double, Boolean, A, Vector]]

      ds2.collect.run() ==
        Seq(X6(x5.a, x5.b, x5.c, x5.d, x5.e, Vectors.dense(x5.a.toDouble, x5.b.toDouble, x5.c, if (x5.d) 1D else 0D)))
    }

    def prop2[A: TypedEncoder: Arbitrary] = forAll { x5: X5[Boolean, BigDecimal, Byte, Short, A] =>
      val assembler = TypedVectorAssembler[X4[Boolean, BigDecimal, Byte, Short]]
      val ds = TypedDataset.create(Seq(x5))
      val ds2 = assembler.transform(ds).run().as[X6[Boolean, BigDecimal, Byte, Short, A, Vector]]

      ds2.collect.run() ==
        Seq(X6(x5.a, x5.b, x5.c, x5.d, x5.e, Vectors.dense(if (x5.a) 1D else 0D, x5.b.toDouble, x5.c.toDouble, x5.d.toDouble)))
    }

    check(prop[String])
    check(prop[Double])
    check(prop2[Long])
    check(prop2[Boolean])
  }

  test("create() compiles only with correct inputs") {
    illTyped("TypedVectorAssembler.create[Double]()")
    illTyped("TypedVectorAssembler.create[X1[String]]()")
    illTyped("TypedVectorAssembler.create[X2[String, Double]]()")
    illTyped("TypedVectorAssembler.create[X3[Int, String, Double]]()")
  }

}
