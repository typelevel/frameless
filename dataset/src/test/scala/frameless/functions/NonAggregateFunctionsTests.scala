package frameless
package functions

import java.io.File
import java.util.Base64
import java.nio.charset.StandardCharsets

import frameless.functions.nonAggregate._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{
  Column,
  Encoder,
  SaveMode,
  functions => sparkFunctions
}
import org.scalacheck.Prop._
import org.scalacheck.{ Arbitrary, Gen, Prop }

import scala.annotation.nowarn

class NonAggregateFunctionsTests extends TypedDatasetSuite {
  val testTempFiles = s"$TEST_OUTPUT_DIR/naFtestoutput"

  object NonNegativeGenerators {

    val doubleGen = for {
      s <- Gen.chooseNum(1, Int.MaxValue)
      e <- Gen.chooseNum(1, Int.MaxValue)
      res: Double = s.toDouble / e.toDouble
    } yield res

    val intGen: Gen[Int] = Gen.chooseNum(1, Int.MaxValue)
    val shortGen: Gen[Short] = Gen.chooseNum(1, Short.MaxValue)
    val longGen: Gen[Long] = Gen.chooseNum(1, Long.MaxValue)
    val byteGen: Gen[Byte] = Gen.chooseNum(1, Byte.MaxValue)
  }

  object NonNegativeArbitraryNumericValues {
    import NonNegativeGenerators._
    implicit val arbInt: Arbitrary[Int] = Arbitrary(intGen)
    implicit val arbDouble: Arbitrary[Double] = Arbitrary(doubleGen)
    implicit val arbLong: Arbitrary[Long] = Arbitrary(longGen)
    implicit val arbShort: Arbitrary[Short] = Arbitrary(shortGen)
    implicit val arbByte: Arbitrary[Byte] = Arbitrary(byteGen)
  }

  private val base64Encoder = Base64.getEncoder

  private def base64X1String(x1: X1[String]): X1[String] = {
    def base64(str: String): String =
      base64Encoder.encodeToString(str.getBytes(StandardCharsets.UTF_8))
    x1.copy(a = base64(x1.a))
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(testTempFiles))
    super.afterAll()
  }

  test("negate") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]],
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[A, B]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.negate(cDS("a")))
        .map(_.getAs[B](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(negate(col)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Byte, Byte] _))
    check(forAll(prop[Short, Short] _))
    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[BigDecimal, java.math.BigDecimal] _))
  }

  test("not") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Boolean]],
        fromBase: Int,
        toBase: Int
      )(implicit
        encX1: Encoder[X1[Boolean]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.not(cDS("a")))
        .map(_.getAs[Boolean](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(not(col)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("conv") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[String]],
        fromBase: Int,
        toBase: Int
      )(implicit
        encX1: Encoder[X1[String]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.conv(cDS("a"), fromBase, toBase))
        .map(_.getAs[String](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res =
        typedDS.select(conv(col, fromBase, toBase)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("degrees") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.degrees(cDS("a")))
        .map(_.getAs[Double](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(degrees(col)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[BigDecimal] _))
  }

  def propBitShift[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
      typedDS: TypedDataset[X1[A]]
    )(typedCol: TypedColumn[X1[A], B],
      sparkFunc: (Column, Int) => Column,
      numBits: Int
    ): Prop = {
    val spark = session
    import spark.implicits._

    val resCompare = typedDS.dataset
      .select(sparkFunc($"a", numBits))
      .map(_.getAs[B](0))
      .collect()
      .toList

    val res = typedDS.select(typedCol).collect().run().toList

    res ?= resCompare
  }

  test("shiftRightUnsigned") {
    val spark = session
    import spark.implicits._

    @nowarn // supress sparkFunctions.shiftRightUnsigned call which is used to maintain Spark 3.1.x backwards compat
    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]],
        numBits: Int
      )(implicit
        catalystBitShift: CatalystBitShift[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propBitShift(typedDS)(
        shiftRightUnsigned(typedDS('a), numBits),
        sparkFunctions.shiftRightUnsigned,
        numBits
      )
    }

    check(forAll(prop[Byte, Int] _))
    check(forAll(prop[Short, Int] _))
    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[BigDecimal, Int] _))
  }

  test("shiftRight") {
    val spark = session
    import spark.implicits._

    @nowarn // supress sparkFunctions.shiftRight call which is used to maintain Spark 3.1.x backwards compat
    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]],
        numBits: Int
      )(implicit
        catalystBitShift: CatalystBitShift[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propBitShift(typedDS)(
        shiftRight(typedDS('a), numBits),
        sparkFunctions.shiftRight,
        numBits
      )
    }

    check(forAll(prop[Byte, Int] _))
    check(forAll(prop[Short, Int] _))
    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[BigDecimal, Int] _))
  }

  test("shiftLeft") {
    val spark = session
    import spark.implicits._

    @nowarn // supress sparkFunctions.shiftLeft call which is used to maintain Spark 3.1.x backwards compat
    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]],
        numBits: Int
      )(implicit
        catalystBitShift: CatalystBitShift[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propBitShift(typedDS)(
        shiftLeft(typedDS('a), numBits),
        sparkFunctions.shiftLeft,
        numBits
      )
    }

    check(forAll(prop[Byte, Int] _))
    check(forAll(prop[Short, Int] _))
    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[BigDecimal, Int] _))
  }

  test("ceil") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystRound[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.ceil(cDS("a")))
        .map(_.getAs[B](0))
        .collect()
        .toList
        .map {
          case bigDecimal: java.math.BigDecimal => bigDecimal.setScale(0)
          case other                            => other
        }
        .asInstanceOf[List[B]]

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(ceil(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int, Long] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[Short, Long] _))
    check(forAll(prop[Double, Long] _))
    check(forAll(prop[BigDecimal, java.math.BigDecimal] _))
  }

  test("sha2") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Array[Byte]]]
      )(implicit
        encX1: Encoder[X1[Array[Byte]]]
      ) = {
      Seq(224, 256, 384, 512).map { numBits =>
        val cDS = session.createDataset(values)
        val resCompare = cDS
          .select(sparkFunctions.sha2(cDS("a"), numBits))
          .map(_.getAs[String](0))
          .collect()
          .toList

        val typedDS = TypedDataset.create(values)
        val res =
          typedDS.select(sha2(typedDS('a), numBits)).collect().run().toList
        res ?= resCompare
      }.reduce(_ && _)
    }

    check(forAll(prop _))
  }

  test("sha1") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Array[Byte]]]
      )(implicit
        encX1: Encoder[X1[Array[Byte]]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.sha1(cDS("a")))
        .map(_.getAs[String](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(sha1(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("crc32") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Array[Byte]]]
      )(implicit
        encX1: Encoder[X1[Array[Byte]]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.crc32(cDS("a")))
        .map(_.getAs[Long](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(crc32(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("floor") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystRound[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.floor(cDS("a")))
        .map(_.getAs[B](0))
        .collect()
        .toList
        .map {
          case bigDecimal: java.math.BigDecimal => bigDecimal.setScale(0)
          case other                            => other
        }
        .asInstanceOf[List[B]]

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(floor(typedDS('a))).collect().run().toList

      res ?= resCompare
    }
    check(forAll(prop[Int, Long] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[Short, Long] _))
    check(forAll(prop[Double, Long] _))
    check(forAll(prop[BigDecimal, java.math.BigDecimal] _))
  }

  test("abs big decimal") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[A, B],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.abs(cDS("a")))
        .map(_.getAs[B](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS
        .select(
          abs(col)
        )
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop[BigDecimal, java.math.BigDecimal] _))
  }

  test("abs") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[A, A],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.abs(cDS("a")))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(abs(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Double] _))
  }

  def propTrigonometric[A: CatalystNumeric: TypedEncoder: Encoder](
      typedDS: TypedDataset[X1[A]]
    )(typedCol: TypedColumn[X1[A], Double],
      sparkFunc: Column => Column
    ): Prop = {
    val spark = session
    import spark.implicits._

    val resCompare = typedDS.dataset
      .select(sparkFunc($"a"))
      .map(_.getAs[Double](0))
      .map(DoubleBehaviourUtils.nanNullHandler)
      .collect()
      .toList

    val res = typedDS
      .select(typedCol)
      .deserialized
      .map(DoubleBehaviourUtils.nanNullHandler)
      .collect()
      .run()
      .toList

    res ?= resCompare
  }

  test("cos") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(cos(typedDS('a)), sparkFunctions.cos)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("cosh") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(cosh(typedDS('a)), sparkFunctions.cosh)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("acos") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(acos(typedDS('a)), sparkFunctions.acos)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("signum") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(signum(typedDS('a)), sparkFunctions.signum)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("sin") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(sin(typedDS('a)), sparkFunctions.sin)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("sinh") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(sinh(typedDS('a)), sparkFunctions.sinh)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("asin") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(asin(typedDS('a)), sparkFunctions.asin)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("tan") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(tan(typedDS('a)), sparkFunctions.tan)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("tanh") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      propTrigonometric(typedDS)(tanh(typedDS('a)), sparkFunctions.tanh)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  /*
   * Currently not all Collection types play nice with the Encoders.
   * This test needs to be readressed and Set readded to the Collection Typeclass once these issues are resolved.
   *
   * [[https://issues.apache.org/jira/browse/SPARK-18891]]
   * [[https://issues.apache.org/jira/browse/SPARK-21204]]
   */
  test("arrayContains") {
    val spark = session
    import spark.implicits._

    val listLength = 10
    val idxs = Stream.continually(Range(0, listLength)).flatten.toIterator

    abstract class Nth[A, C[A]: CatalystCollection] {

      def nth(c: C[A], idx: Int): A
    }

    implicit def deriveListNth[A]: Nth[A, List] = new Nth[A, List] {
      override def nth(c: List[A], idx: Int): A = c(idx)
    }

    implicit def deriveSeqNth[A]: Nth[A, Seq] = new Nth[A, Seq] {
      override def nth(c: Seq[A], idx: Int): A = c(idx)
    }

    implicit def deriveVectorNth[A]: Nth[A, Vector] = new Nth[A, Vector] {
      override def nth(c: Vector[A], idx: Int): A = c(idx)
    }

    implicit def deriveArrayNth[A]: Nth[A, Array] = new Nth[A, Array] {
      override def nth(c: Array[A], idx: Int): A = c(idx)
    }

    def prop[C[_]: CatalystCollection](
        values: C[Int],
        shouldBeIn: Boolean
      )(implicit
        nth: Nth[Int, C],
        encEv: Encoder[C[Int]],
        tEncEv: TypedEncoder[C[Int]]
      ) = {

      val contained = if (shouldBeIn) nth.nth(values, idxs.next) else -1

      val cDS = session.createDataset(List(values))
      val resCompare = cDS
        .select(sparkFunctions.array_contains(cDS("value"), contained))
        .map(_.getAs[Boolean](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(List(X1(values)))
      val res = typedDS
        .select(arrayContains(typedDS('a), contained))
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(
      forAll(
        Gen.listOfN(listLength, Gen.choose(0, 100)),
        Gen.oneOf(true, false)
      )(prop[List])
    )

    /*check( Looks like there is no Typed Encoder for Seq type yet
      forAll(
        Gen.listOfN(listLength, Gen.choose(0,100)),
        Gen.oneOf(true,false)
      )
      (prop[Seq])
    )*/

    check(
      forAll(
        Gen.listOfN(listLength, Gen.choose(0, 100)).map(_.toVector),
        Gen.oneOf(true, false)
      )(prop[Vector])
    )

    check(
      forAll(
        Gen.listOfN(listLength, Gen.choose(0, 100)).map(_.toArray),
        Gen.oneOf(true, false)
      )(prop[Array])
    )
  }

  test("atan") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder: CatalystOrdered](
        na: A,
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(X1(na) :: values)
      val resCompare = cDS
        .select(sparkFunctions.atan(cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList
        .sorted

      val typedDS = TypedDataset.create(cDS)
      val res = typedDS
        .select(atan(typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList
        .sorted

      val aggrTyped = typedDS
        .coalesce(1)
        .orderBy(typedDS('a).asc)
        .agg(atan(frameless.functions.aggregate.first(typedDS('a))))
        .firstOption()
        .run()
        .get

      val aggrSpark = cDS
        .coalesce(1)
        .orderBy("a")
        .select(
          sparkFunctions.atan(sparkFunctions.first("a")).as[Double]
        )
        .first()

      (res ?= resCompare).&&(
        DoubleBehaviourUtils.nanNullHandler(aggrTyped) ?= DoubleBehaviourUtils
          .nanNullHandler(aggrSpark)
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("atan2") {
    val spark = session
    import spark.implicits._

    def prop[
        A: CatalystNumeric: TypedEncoder: Encoder: CatalystOrdered,
        B: CatalystNumeric: TypedEncoder: Encoder: CatalystOrdered
      ](na: X2[A, B],
        values: List[X2[A, B]]
      )(implicit
        encEv: Encoder[X2[A, B]]
      ) = {
      val cDS = session.createDataset(na +: values)
      val resCompare = cDS
        .select(sparkFunctions.atan2(cDS("a"), cDS("b")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList
        .sorted

      val typedDS = TypedDataset.create(cDS)
      val res = typedDS
        .select(atan2(typedDS('a), typedDS('b)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList
        .sorted

      val aggrTyped = typedDS
        .coalesce(1)
        .orderBy(typedDS('a).asc, typedDS('b).asc)
        .agg(
          atan2(
            frameless.functions.aggregate.first(typedDS('a)),
            frameless.functions.aggregate.first(typedDS('b))
          )
        )
        .firstOption()
        .run()
        .get

      val aggrSpark = cDS
        .coalesce(1)
        .orderBy("a", "b")
        .select(
          sparkFunctions
            .atan2(sparkFunctions.first("a"), sparkFunctions.first("b"))
            .as[Double]
        )
        .first()

      (res ?= resCompare).&&(
        DoubleBehaviourUtils.nanNullHandler(aggrTyped) ?= DoubleBehaviourUtils
          .nanNullHandler(aggrSpark)
      )
    }

    check(forAll(prop[Int, Long] _))
    check(forAll(prop[Long, Int] _))
    check(forAll(prop[Short, Byte] _))
    check(forAll(prop[BigDecimal, Double] _))
    check(forAll(prop[Byte, Int] _))
    check(forAll(prop[Double, Double] _))
  }

  test("atan2LitLeft") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder: CatalystOrdered](
        na: X1[A],
        value: List[X1[A]],
        lit: Double
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(na +: value)
      val resCompare = cDS
        .select(sparkFunctions.atan2(lit, cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList
        .sorted

      val typedDS = TypedDataset.create(cDS)
      val res = typedDS
        .select(atan2(lit, typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList
        .sorted

      val aggrTyped = typedDS
        .coalesce(1)
        .orderBy(typedDS('a).asc)
        .agg(atan2(lit, frameless.functions.aggregate.first(typedDS('a))))
        .firstOption()
        .run()
        .get

      val aggrSpark = cDS
        .coalesce(1)
        .orderBy("a")
        .select(
          sparkFunctions.atan2(lit, sparkFunctions.first("a")).as[Double]
        )
        .first()

      (res ?= resCompare).&&(
        DoubleBehaviourUtils.nanNullHandler(aggrTyped) ?= DoubleBehaviourUtils
          .nanNullHandler(aggrSpark)
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("atan2LitRight") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder: CatalystOrdered](
        na: X1[A],
        value: List[X1[A]],
        lit: Double
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(na +: value)
      val resCompare = cDS
        .select(sparkFunctions.atan2(cDS("a"), lit))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList
        .sorted

      val typedDS = TypedDataset.create(cDS)
      val res = typedDS
        .select(atan2(typedDS('a), lit))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList
        .sorted

      val aggrTyped = typedDS
        .coalesce(1)
        .orderBy(typedDS('a).asc)
        .agg(atan2(frameless.functions.aggregate.first(typedDS('a)), lit))
        .firstOption()
        .run()
        .get

      val aggrSpark = cDS
        .coalesce(1)
        .orderBy("a")
        .select(
          sparkFunctions.atan2(sparkFunctions.first("a"), lit).as[Double]
        )
        .first()

      (res ?= resCompare).&&(
        DoubleBehaviourUtils.nanNullHandler(aggrTyped) ?= DoubleBehaviourUtils
          .nanNullHandler(aggrSpark)
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  def mathProp[A: CatalystNumeric: TypedEncoder: Encoder](
      typedDS: TypedDataset[X1[A]]
    )(typedCol: TypedColumn[X1[A], Double],
      sparkFunc: Column => Column
    ): Prop = {
    val spark = session
    import spark.implicits._

    val resCompare = typedDS.dataset
      .select(sparkFunc($"a"))
      .map(_.getAs[Double](0))
      .map(DoubleBehaviourUtils.nanNullHandler)
      .collect()
      .toList

    val res = typedDS
      .select(typedCol)
      .deserialized
      .map(DoubleBehaviourUtils.nanNullHandler)
      .collect()
      .run()
      .toList

    res ?= resCompare
  }

  test("sqrt") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(sqrt(typedDS('a)), sparkFunctions.sqrt)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("crbt") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(cbrt(typedDS('a)), sparkFunctions.cbrt)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("exp") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(exp(typedDS('a)), sparkFunctions.exp)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("md5") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](values: List[X1[A]]): Prop = {
      val spark = session
      import spark.implicits._

      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.md5($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toList

      val res = typedDS.select(md5(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[String] _))
  }

  test("factorial") {
    val spark = session

    def prop(values: List[X1[Long]]): Prop = {
      val spark = session
      import spark.implicits._

      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.factorial($"a"))
        .map(_.getAs[Long](0))
        .collect()
        .toList

      val res = typedDS.select(factorial(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("round") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystNumericWithJavaBigDecimal: CatalystNumericWithJavaBigDecimal[
          A,
          A
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.round(cDS("a")))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(round(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Double] _))
  }

  test("round big decimal") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[
          A,
          java.math.BigDecimal
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.round(cDS("a")))
        .map(_.getAs[java.math.BigDecimal](0))
        .collect()
        .toList
        .map(_.setScale(0))

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(round(col)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[BigDecimal] _))
  }

  test("round with scale") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystNumericWithJavaBigDecimal: CatalystNumericWithJavaBigDecimal[
          A,
          A
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.round(cDS("a"), 1))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(round(typedDS('a), 1)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Double] _))
  }

  test("round big decimal with scale") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[
          A,
          java.math.BigDecimal
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.round(cDS("a"), 0))
        .map(_.getAs[java.math.BigDecimal](0))
        .collect()
        .toList
        .map(_.setScale(0))

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(round(col, 0)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[BigDecimal] _))
  }

  test("bround") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystNumericWithJavaBigDecimal: CatalystNumericWithJavaBigDecimal[
          A,
          A
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.bround(cDS("a")))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(bround(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Double] _))
  }

  test("bround big decimal") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[
          A,
          java.math.BigDecimal
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.bround(cDS("a")))
        .map(_.getAs[java.math.BigDecimal](0))
        .collect()
        .toList
        .map(_.setScale(0))

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(bround(col)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[BigDecimal] _))
  }

  test("bround with scale") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystNumericWithJavaBigDecimal: CatalystNumericWithJavaBigDecimal[
          A,
          A
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.bround(cDS("a"), 1))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(bround(typedDS('a), 1)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Double] _))
  }

  test("bround big decimal with scale") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        catalystAbsolute: CatalystNumericWithJavaBigDecimal[
          A,
          java.math.BigDecimal
        ],
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)

      val resCompare = cDS
        .select(sparkFunctions.bround(cDS("a"), 0))
        .map(_.getAs[java.math.BigDecimal](0))
        .collect()
        .toList
        .map(_.setScale(0))

      val typedDS = TypedDataset.create(values)
      val col = typedDS('a)
      val res = typedDS.select(bround(col, 0)).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[BigDecimal] _))
  }

  test("log with base") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]],
        base: Double
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.log(base, $"a"))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res = typedDS
        .select(log(base, typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("log") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(log(typedDS('a)), sparkFunctions.log)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("log2") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(log2(typedDS('a)), sparkFunctions.log2)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("log1p") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(log1p(typedDS('a)), sparkFunctions.log1p)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("log10") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val typedDS = TypedDataset.create(values)
      mathProp(typedDS)(log10(typedDS('a)), sparkFunctions.log10)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("base64") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Array[Byte]]]
      )(implicit
        encX1: Encoder[X1[Array[Byte]]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.base64(cDS("a")))
        .map(_.getAs[String](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(base64(typedDS('a))).collect().run().toList

      val backAndForth = typedDS
        .select(base64(unbase64(base64(typedDS('a)))))
        .collect()
        .run()
        .toList

      (res ?= resCompare) && (res ?= backAndForth)
    }

    check(forAll(prop _))
  }

  test("hypot with double") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]],
        base: Double
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.hypot(base, $"a"))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res2 = typedDS
        .select(hypot(typedDS('a), base))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      val res = typedDS
        .select(hypot(base, typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      (res ?= resCompare) && (res2 ?= resCompare)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("hypot with two columns") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X2[A, A]]
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.hypot($"b", $"a"))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res = typedDS
        .select(hypot(typedDS('b), typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("pow with double") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X1[A]],
        base: Double
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.pow(base, $"a"))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res = typedDS
        .select(pow(base, typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      val resCompare2 = typedDS.dataset
        .select(sparkFunctions.pow($"a", base))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res2 = typedDS
        .select(pow(typedDS('a), base))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      (res ?= resCompare) && (res2 ?= resCompare2)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("pow with two columns") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X2[A, A]]
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.pow($"b", $"a"))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .toList

      val res = typedDS
        .select(pow(typedDS('b), typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("pmod") {
    val spark = session
    import spark.implicits._
    import NonNegativeArbitraryNumericValues._

    def prop[A: CatalystNumeric: TypedEncoder: Encoder](
        values: List[X2[A, A]]
      ): Prop = {
      val spark = session
      import spark.implicits._
      val typedDS = TypedDataset.create(values)

      val resCompare = typedDS.dataset
        .select(sparkFunctions.pmod($"b", $"a"))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val res =
        typedDS.select(pmod(typedDS('b), typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("unbase64") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[String]]
      )(implicit
        encX1: Encoder[X1[String]]
      ) = {
      val valuesBase64 = values.map(base64X1String)
      val cDS = session.createDataset(valuesBase64)
      val resCompare = cDS
        .select(sparkFunctions.unbase64(cDS("a")))
        .map(_.getAs[Array[Byte]](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(valuesBase64)
      val res = typedDS.select(unbase64(typedDS('a))).collect().run().toList

      res.map(_.toList) ?= resCompare.map(_.toList)
    }

    check(forAll(prop _))
  }

  test("bin") {
    val spark = session
    import spark.implicits._

    def prop(
        values: List[X1[Long]]
      )(implicit
        encX1: Encoder[X1[Long]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.bin(cDS("a")))
        .map(_.getAs[String](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(bin(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("bitwiseNOT") {
    val spark = session
    import spark.implicits._

    @nowarn // supress sparkFunctions.bitwiseNOT call which is used to maintain Spark 3.1.x backwards compat
    def prop[A: CatalystBitwise: TypedEncoder: Encoder](
        values: List[X1[A]]
      )(implicit
        encX1: Encoder[X1[A]]
      ) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(sparkFunctions.bitwiseNOT(cDS("a")))
        .map(_.getAs[A](0))
        .collect()
        .toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS.select(bitwiseNOT(typedDS('a))).collect().run().toList

      res ?= resCompare
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Int] _))
  }

  test("inputFileName") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder](
        toFile1: List[X1[A]],
        toFile2: List[X1[A]],
        inMem: List[X1[A]]
      )(implicit
        x2Gen: Encoder[X2[A, String]],
        x3Gen: Encoder[X3[A, String, String]]
      ) = {

      val file1Path = testTempFiles + "/file1"
      val file2Path = testTempFiles + "/file2"

      val toFile1WithName = toFile1.map(x => X2(x.a, "file1"))
      val toFile2WithName = toFile2.map(x => X2(x.a, "file2"))
      val inMemWithName = inMem.map(x => X2(x.a, ""))

      toFile1WithName.toDS().write.mode(SaveMode.Overwrite).parquet(file1Path)
      toFile2WithName.toDS().write.mode(SaveMode.Overwrite).parquet(file2Path)

      val readBackIn1 = spark.read.parquet(file1Path).as[X2[A, String]]
      val readBackIn2 = spark.read.parquet(file2Path).as[X2[A, String]]

      val ds1 = TypedDataset.create(readBackIn1)
      val ds2 = TypedDataset.create(readBackIn2)
      val ds3 = TypedDataset.create(inMemWithName)

      val unioned = ds1.union(ds2).union(ds3)

      val withFileName = unioned
        .withColumn[X3[A, String, String]](inputFileName[X2[A, String]]())
        .collect()
        .run()
        .toVector

      val grouped = withFileName.groupBy(_.b).mapValues(_.map(_.c).toSet)

      grouped.foldLeft(passed) { (p, g) =>
        p && secure {
          g._1 match {
            case "" => g._2.head == "" // Empty string if didn't come from file
            case f  => g._2.forall(_.contains(f))
          }
        }
      }
    }

    check(forAll(prop[String] _))
  }

  test("monotonic id") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder](
        xs: List[X1[A]]
      )(implicit
        x2en: Encoder[X2[A, Long]]
      ) = {
      val ds = TypedDataset.create(xs)

      val result = ds
        .withColumn[X2[A, Long]](monotonicallyIncreasingId())
        .collect()
        .run()
        .toVector

      val ids = result.map(_.b)
      (ids.toSet.size ?= ids.length) &&
      (ids.sorted ?= ids)
    }

    check(forAll(prop[String] _))
  }

  test("when") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder](
        condition1: Boolean,
        condition2: Boolean,
        value1: A,
        value2: A,
        otherwise: A
      ) = {
      val ds = TypedDataset.create(
        X5(condition1, condition2, value1, value2, otherwise) :: Nil
      )

      val untypedWhen = ds
        .toDF()
        .select(
          sparkFunctions
            .when(sparkFunctions.col("a"), sparkFunctions.col("c"))
            .when(sparkFunctions.col("b"), sparkFunctions.col("d"))
            .otherwise(sparkFunctions.col("e"))
        )
        .as[A]
        .collect()
        .toList

      val typedWhen = ds
        .select(
          when(ds('a), ds('c)).when(ds('b), ds('d)).otherwise(ds('e))
        )
        .collect()
        .run()
        .toList

      typedWhen ?= untypedWhen
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Option[Int]] _))
  }

  test("ascii") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.ascii($"a"))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds.select(ascii(ds('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("concat") {
    val spark = session
    import spark.implicits._

    val pairs = for {
      y <- Gen.alphaStr
      x <- Gen.nonEmptyListOf(X2(y, y))
    } yield x

    check(forAll(pairs) { values: List[X2[String, String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.concat($"a", $"b"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(concat(ds('a), ds('b))).collect().run().toVector

      (typed ?= sparkResult).&&(
        typed ?= values.map(x => s"${x.a}${x.b}").toVector
      )
    })
  }

  test("concat for TypedAggregate") {
    val spark = session
    import frameless.functions.aggregate._
    import spark.implicits._
    val pairs = for {
      y <- Gen.alphaStr
      x <- Gen.nonEmptyListOf(X2(y, y))
    } yield x

    check(forAll(pairs) { values: List[X2[String, String]] =>
      val ds = TypedDataset.create(values)
      val td =
        ds.coalesce(1)
          .agg(concat(first(ds('a)), first(ds('b))))
          .collect()
          .run()
          .toVector
      val spark = ds.dataset
        .coalesce(1)
        .select(
          sparkFunctions.concat(
            sparkFunctions.first($"a").as[String],
            sparkFunctions.first($"b").as[String]
          )
        )
        .as[String]
        .collect()
        .toVector
      td ?= spark
    })
  }

  test("concat_ws") {
    val spark = session
    import spark.implicits._

    val pairs = for {
      y <- Gen.alphaStr
      x <- Gen.nonEmptyListOf(X2(y, y))
    } yield x

    check(forAll(pairs) { values: List[X2[String, String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.concat_ws(",", $"a", $"b"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed =
        ds.select(concatWs(",", ds('a), ds('b))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("concat_ws for TypedAggregate") {
    val spark = session
    import frameless.functions.aggregate._
    import spark.implicits._
    val pairs = for {
      y <- Gen.alphaStr
      x <- Gen.listOfN(10, X2(y, y))
    } yield x

    check(forAll(pairs) { values: List[X2[String, String]] =>
      val ds = TypedDataset.create(values)
      val td = ds
        .coalesce(1)
        .agg(concatWs(",", first(ds('a)), first(ds('b)), last(ds('b))))
        .collect()
        .run()
        .toVector
      val spark = ds.dataset
        .coalesce(1)
        .select(
          sparkFunctions.concat_ws(
            ",",
            sparkFunctions.first($"a").as[String],
            sparkFunctions.first($"b").as[String],
            sparkFunctions.last($"b").as[String]
          )
        )
        .as[String]
        .collect()
        .toVector
      td ?= spark
    })
  }

  test("instr") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.nonEmptyListOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(x => X1(x + values.head)))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.instr($"a", values.head))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds.select(instr(ds('a), values.head)).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("length") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.length($"a"))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds.select(length(ds[String]('a))).collect().run().toVector

      (typed ?= sparkResult).&&(values.map(_.a.length).toVector ?= typed)
    })
  }

  test("levenshtein") {
    val spark = session
    import spark.implicits._
    check(forAll { (na: X1[String], values: List[X1[String]]) =>
      val ds = TypedDataset.create(na +: values)

      val sparkResult = ds
        .toDF()
        .select(
          sparkFunctions.levenshtein(
            $"a",
            sparkFunctions.concat($"a", sparkFunctions.lit("Hello"))
          )
        )
        .map(_.getAs[Int](0))
        .collect()
        .toVector
        .sorted

      val typed = ds
        .select(levenshtein(ds('a), concat(ds('a), lit("Hello"))))
        .collect()
        .run()
        .toVector
        .sorted

      val cDS = ds.dataset
      val aggrTyped = ds
        .coalesce(1)
        .orderBy(ds('a).asc)
        .agg(
          levenshtein(
            frameless.functions.aggregate.first(ds('a)),
            litAggr("Hello")
          )
        )
        .firstOption()
        .run()
        .get

      val aggrSpark = cDS
        .coalesce(1)
        .orderBy("a")
        .select(
          sparkFunctions
            .levenshtein(sparkFunctions.first("a"), sparkFunctions.lit("Hello"))
            .as[Int]
        )
        .first()

      (typed ?= sparkResult).&&(aggrTyped ?= aggrSpark)
    })
  }

  test("regexp_replace") {
    val spark = session
    import spark.implicits._
    check(forAll { (values: List[X1[String]], n: Int) =>
      val ds = TypedDataset.create(values.map(x => X1(s"$n${x.a}-$n$n")))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.regexp_replace($"a", "\\d+", "n"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(regexpReplace(ds[String]('a), "\\d+".r, "n"))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("reverse") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.reverse($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(reverse(ds[String]('a))).collect().run().toVector

      (typed ?= sparkResult).&&(values.map(_.a.reverse).toVector ?= typed)
    })
  }

  test("rpad") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.rpad($"a", 5, "hello"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed =
        ds.select(rpad(ds[String]('a), 5, "hello")).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("lpad") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.lpad($"a", 5, "hello"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed =
        ds.select(lpad(ds[String]('a), 5, "hello")).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("rtrim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.rtrim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(rtrim(ds[String]('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("ltrim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.ltrim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(ltrim(ds[String]('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("substring") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.substring($"a", 5, 3))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed =
        ds.select(substring(ds[String]('a), 5, 3)).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("trim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.trim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(trim(ds[String]('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("upper") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.listOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(X1(_)))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.upper($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(upper(ds[String]('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("lower") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.listOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(X1(_)))

      val sparkResult = ds
        .toDF()
        .select(sparkFunctions.lower($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds.select(lower(ds[String]('a))).collect().run().toVector

      typed ?= sparkResult
    })
  }

  test("Empty vararg tests") {
    def prop[A: TypedEncoder, B: TypedEncoder](data: Vector[X2[A, B]]) = {
      val ds = TypedDataset.create(data)
      val frameless = ds
        .select(ds('a), concat(), ds('b), concatWs(":"))
        .collect()
        .run()
        .toVector
      val framelessAggr =
        ds.agg(concat(), concatWs("x"), litAggr(2)).collect().run().toVector
      val scala = data.map(x => (x.a, "", x.b, ""))
      val scalaAggr = Vector(("", "", 2))
      (frameless ?= scala).&&(framelessAggr ?= scalaAggr)
    }

    check(forAll(prop[Long, Long] _))
    check(forAll(prop[Option[Boolean], Long] _))
  }

  def dateTimeStringProp(
      typedDS: TypedDataset[X1[String]]
    )(typedCol: TypedColumn[X1[String], Option[Int]],
      sparkFunc: Column => Column
    ): Prop = {
    val spark = session
    import spark.implicits._

    val sparkResult = typedDS.dataset
      .select(sparkFunc($"a"))
      .map(DateTimeStringBehaviourUtils.nullHandler)
      .collect()
      .toList

    val typed = typedDS.select(typedCol).collect().run().toList

    typed ?= sparkResult
  }

  test("year") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(year(ds[String]('a)), sparkFunctions.year)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("quarter") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(quarter(ds[String]('a)), sparkFunctions.quarter)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("month") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(month(ds[String]('a)), sparkFunctions.month)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("dayofweek") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(
        dayofweek(ds[String]('a)),
        sparkFunctions.dayofweek
      )
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("dayofmonth") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(
        dayofmonth(ds[String]('a)),
        sparkFunctions.dayofmonth
      )
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("dayofyear") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(
        dayofyear(ds[String]('a)),
        sparkFunctions.dayofyear
      )
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("hour") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(hour(ds[String]('a)), sparkFunctions.hour)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("minute") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(minute(ds[String]('a)), sparkFunctions.minute)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("second") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(second(ds[String]('a)), sparkFunctions.second)
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }

  test("weekofyear") {
    val spark = session
    import spark.implicits._

    def prop(
        data: List[X1[String]]
      )(implicit
        E: Encoder[Option[Int]]
      ): Prop = {
      val ds = TypedDataset.create(data)
      dateTimeStringProp(ds)(
        weekofyear(ds[String]('a)),
        sparkFunctions.weekofyear
      )
    }

    check(forAll(dateTimeStringGen)(data => prop(data.map(X1.apply))))
    check(forAll(prop _))
  }
}
