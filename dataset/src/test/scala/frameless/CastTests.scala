// package frameless

// import org.scalacheck.{Arbitrary, Gen, Prop}
// import org.scalacheck.Prop._

// class CastTests extends TypedDatasetSuite {

//   def prop[A: TypedEncoder, B: TypedEncoder](f: A => B)(a: A)(
//     implicit
//     cast: CatalystCast[A, B]
//   ): Prop = {
//     val df = TypedDataset.create(X1(a) :: Nil)
//     val got = df.select(df.col('a).cast[B]).collect().run()

//     got ?= (f(a) :: Nil)
//   }

//   test("cast") {
//     // numericToDecimal
//     check(prop[BigDecimal, BigDecimal](identity) _)
//     check(prop[Byte, BigDecimal](x => BigDecimal.valueOf(x.toLong)) _)
//     check(prop[Double, BigDecimal](BigDecimal.valueOf) _)
//     check(prop[Int, BigDecimal](x => BigDecimal.valueOf(x.toLong)) _)
//     check(prop[Long, BigDecimal](BigDecimal.valueOf) _)
//     check(prop[Short, BigDecimal](x => BigDecimal.valueOf(x.toLong)) _)

//     // numericToByte
//     check(prop[BigDecimal, Byte](_.toByte) _)
//     check(prop[Byte, Byte](identity) _)
//     check(prop[Double, Byte](_.toByte) _)
//     check(prop[Int, Byte](_.toByte) _)
//     check(prop[Long, Byte](_.toByte) _)
//     check(prop[Short, Byte](_.toByte) _)

//     // numericToDouble
//     check(prop[BigDecimal, Double](_.toDouble) _)
//     check(prop[Byte, Double](_.toDouble) _)
//     check(prop[Double, Double](identity) _)
//     check(prop[Int, Double](_.toDouble) _)
//     check(prop[Long, Double](_.toDouble) _)
//     check(prop[Short, Double](_.toDouble) _)

//     // numericToInt
//     check(prop[BigDecimal, Int](_.toInt) _)
//     check(prop[Byte, Int](_.toInt) _)
//     check(prop[Double, Int](_.toInt) _)
//     check(prop[Int, Int](identity) _)
//     check(prop[Long, Int](_.toInt) _)
//     check(prop[Short, Int](_.toInt) _)

//     // numericToLong
//     check(prop[BigDecimal, Long](_.toLong) _)
//     check(prop[Byte, Long](_.toLong) _)
//     check(prop[Double, Long](_.toLong) _)
//     check(prop[Int, Long](_.toLong) _)
//     check(prop[Long, Long](identity) _)
//     check(prop[Short, Long](_.toLong) _)

//     // numericToShort
//     check(prop[BigDecimal, Short](_.toShort) _)
//     check(prop[Byte, Short](_.toShort) _)
//     check(prop[Double, Short](_.toShort) _)
//     check(prop[Int, Short](_.toShort) _)
//     check(prop[Long, Short](_.toShort) _)
//     check(prop[Short, Short](identity) _)

//     // castToString
//     // TODO compare without trailing zeros
//     // check(prop[BigDecimal, String](_.toString()) _)
//     check(prop[Byte, String](_.toString) _)
//     check(prop[Double, String](_.toString) _)
//     check(prop[Int, String](_.toString) _)
//     check(prop[Long, String](_.toString) _)
//     check(prop[Short, String](_.toString) _)

//     // stringToBoolean
//     val trueStrings = Set("t", "true", "y", "yes", "1")
//     val falseStrings = Set("f", "false", "n", "no", "0")

//     def stringToBoolean(str: String): Option[Boolean] = {
//       if (trueStrings(str)) Some(true)
//       else if (falseStrings(str)) Some(false)
//       else None
//     }

//     val stringToBooleanGen = Gen.oneOf(
//       Gen.oneOf(trueStrings.toSeq),
//       Gen.oneOf(falseStrings.toSeq),
//       Arbitrary.arbitrary[String]
//     )

//     check(forAll(stringToBooleanGen)(prop(stringToBoolean)))

//     // xxxToBoolean
//     check(prop[BigDecimal, Boolean](_ != BigDecimal(0)) _)
//     check(prop[Byte, Boolean](_ != 0) _)
//     check(prop[Double, Boolean](_ != 0) _)
//     check(prop[Int, Boolean](_ != 0) _)
//     check(prop[Long, Boolean](_ != 0L) _)
//     check(prop[Short, Boolean](_ != 0) _)

//     // booleanToNumeric
//     check(prop[Boolean, BigDecimal](x => if (x) BigDecimal(1) else BigDecimal(0)) _)
//     check(prop[Boolean, Byte](x => if (x) 1 else 0) _)
//     check(prop[Boolean, Double](x => if (x) 1.0f else 0.0f) _)
//     check(prop[Boolean, Int](x => if (x) 1 else 0) _)
//     check(prop[Boolean, Long](x => if (x) 1L else 0L) _)
//     check(prop[Boolean, Short](x => if (x) 1 else 0) _)
//   }

// }
