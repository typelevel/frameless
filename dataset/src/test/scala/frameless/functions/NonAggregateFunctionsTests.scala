package frameless
package functions

import java.io.File

import frameless.functions.nonAggregate._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Encoder, SaveMode, functions => untyped}
import org.scalacheck.Gen
import org.scalacheck.Prop._

class NonAggregateFunctionsTests extends TypedDatasetSuite {
  val testTempFiles = "target/testoutput"

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(testTempFiles))
    super.afterAll()
  }

  test("abs big decimal") {
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder: Encoder, B: TypedEncoder: Encoder]
      (values: List[X1[A]])
      (
        implicit catalystAbsolute: CatalystAbsolute[A, B],
        encX1:Encoder[X1[A]]
      )= {
        val cDS = session.createDataset(values)
        val resCompare = cDS
          .select(org.apache.spark.sql.functions.abs(cDS("a")))
          .map(_.getAs[B](0))
          .collect().toList


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

    def prop[A: TypedEncoder : Encoder]
      (values: List[X1[A]])
      (
        implicit catalystAbsolute: CatalystAbsolute[A, A],
        encX1:Encoder[X1[A]]
      )= {
        val cDS = session.createDataset(values)
        val resCompare = cDS
          .select(org.apache.spark.sql.functions.abs(cDS("a")))
          .map(_.getAs[A](0))
          .collect().toList


        val typedDS = TypedDataset.create(values)
        val res = typedDS
          .select(abs(typedDS('a)))
          .collect()
          .run()
          .toList

        res ?= resCompare
      }

      check(forAll(prop[Int] _))
      check(forAll(prop[Long] _))
      check(forAll(prop[Short] _))
      check(forAll(prop[Double] _))
    }



  test("acos") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder]
    (values: List[X1[A]])
    (implicit encX1:Encoder[X1[A]])
    = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.acos(cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(acos(typedDS('a)))
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
  test("arrayContains"){


    val spark = session
    import spark.implicits._

    val listLength = 10
    val idxs = Stream.continually(Range(0, listLength)).flatten.toIterator

    abstract class Nth[A, C[A]:CatalystCollection] {

      def nth(c:C[A], idx:Int):A
    }

    implicit def deriveListNth[A] : Nth[A, List] = new Nth[A, List] {
      override def nth(c: List[A], idx: Int): A = c(idx)
    }

    implicit def deriveSeqNth[A] : Nth[A, Seq] = new Nth[A, Seq] {
      override def nth(c: Seq[A], idx: Int): A = c(idx)
    }

    implicit def deriveVectorNth[A] : Nth[A, Vector] = new Nth[A, Vector] {
      override def nth(c: Vector[A], idx: Int): A = c(idx)
    }

    implicit def deriveArrayNth[A] : Nth[A, Array] = new Nth[A, Array] {
      override def nth(c: Array[A], idx: Int): A = c(idx)
    }


    def prop[C[_] : CatalystCollection]
      (
        values: C[Int],
        shouldBeIn:Boolean)
      (
        implicit nth:Nth[Int, C],
        encEv: Encoder[C[Int]],
        tEncEv: TypedEncoder[C[Int]]
      ) = {

      val contained = if (shouldBeIn) nth.nth(values, idxs.next) else -1

      val cDS = session.createDataset(List(values))
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.array_contains(cDS("value"), contained))
        .map(_.getAs[Boolean](0))
        .collect().toList


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
        Gen.listOfN(listLength, Gen.choose(0,100)),
        Gen.oneOf(true,false)
      )
      (prop[List])
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
        Gen.listOfN(listLength, Gen.choose(0,100)).map(_.toVector),
        Gen.oneOf(true,false)
      )
      (prop[Vector])
    )

    check(
      forAll(
        Gen.listOfN(listLength, Gen.choose(0,100)).map(_.toArray),
        Gen.oneOf(true,false)
      )
      (prop[Array])
    )
  }

  test("atan") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](values: List[X1[A]])(implicit encX1:Encoder[X1[A]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan(cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(atan(typedDS('a)))
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
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("asin") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](values: List[X1[A]])(implicit encX1:Encoder[X1[A]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.asin(cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(asin(typedDS('a)))
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
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("atan2") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder, B: CatalystNumeric : TypedEncoder : Encoder](values: List[X2[A, B]])
            (implicit encEv: Encoder[X2[A,B]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(cDS("a"), cDS("b")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(atan2(typedDS('a), typedDS('b)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      res ?= resCompare
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

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[X1[A]], lit:Double)(implicit encX1:Encoder[X1[A]]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(lit, cDS("a")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value)
      val res = typedDS
        .select(atan2(lit, typedDS('a)))
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
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("atan2LitRight") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[X1[A]], lit:Double)(implicit encX1:Encoder[X1[A]]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(cDS("a"), lit))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value)
      val res = typedDS
        .select(atan2(typedDS('a), lit))
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
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("base64") {
    val spark = session
    import spark.implicits._

    def prop(values:List[X1[Array[Byte]]])(implicit encX1:Encoder[X1[Array[Byte]]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.base64(cDS("a")))
        .map(_.getAs[String](0))
        .collect().toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(base64(typedDS('a)))
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }

  test("bin"){
    val spark = session
    import spark.implicits._

    def prop(values:List[X1[Long]])(implicit encX1:Encoder[X1[Long]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.bin(cDS("a")))
        .map(_.getAs[String](0))
        .collect().toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(bin(typedDS('a)))
        .collect()
        .run()
        .toList

      res ?= resCompare
    }

    check(forAll(prop _))
  }


  test("bitwiseNOT"){
    val spark = session
    import spark.implicits._

    def prop[A: CatalystBitwise : TypedEncoder : Encoder](values:List[X1[A]])(implicit encX1:Encoder[X1[A]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.bitwiseNOT(cDS("a")))
        .map(_.getAs[A](0))
        .collect().toList

      val typedDS = TypedDataset.create(values)
      val res = typedDS
        .select(bitwiseNOT(typedDS('a)))
        .collect()
        .run()
        .toList

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

    def prop[A : TypedEncoder](
      toFile1: List[X1[A]],
      toFile2: List[X1[A]],
      inMem: List[X1[A]]
    )(implicit x2Gen: Encoder[X2[A, String]], x3Gen: Encoder[X3[A, String, String]]) = {

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

      val withFileName = unioned.withColumn[X3[A, String, String]](inputFileName[X2[A, String]]())
        .collect()
        .run()
        .toVector

      val grouped = withFileName.groupBy(_.b).mapValues(_.map(_.c).toSet)

      grouped.foldLeft(passed) { (p, g) =>
        p && secure { g._1 match {
          case "" => g._2.head == "" //Empty string if didn't come from file
          case f => g._2.forall(_.contains(f))
        }}}
    }

    check(forAll(prop[String] _))
  }

  test("monotonic id") {
    val spark = session
    import spark.implicits._

    def prop[A : TypedEncoder](xs: List[X1[A]])(implicit x2en: Encoder[X2[A, Long]]) = {
      val ds = TypedDataset.create(xs)

      val result = ds.withColumn[X2[A, Long]](monotonicallyIncreasingId())
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

    def prop[A : TypedEncoder : Encoder](condition1: Boolean, condition2: Boolean, value1: A, value2: A, otherwise: A) = {
      val ds = TypedDataset.create(X5(condition1, condition2, value1, value2, otherwise) :: Nil)

      val untypedWhen = ds.toDF()
        .select(
          untyped.when(untyped.col("a"), untyped.col("c"))
            .when(untyped.col("b"), untyped.col("d"))
            .otherwise(untyped.col("e"))
        )
        .as[A]
        .collect()
        .toList

      val typedWhen = ds
        .select(
          when(ds('a), ds('c))
            .when(ds('b), ds('d))
            .otherwise(ds('e))
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

      val sparkResult = ds.toDF()
        .select(untyped.ascii($"a"))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds
        .select(ascii(ds('a)))
        .collect()
        .run()
        .toVector

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

      val sparkResult = ds.toDF()
        .select(untyped.concat($"a", $"b"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(concat(ds('a), ds('b)))
        .collect()
        .run()
        .toVector

      (typed ?= sparkResult).&&(typed ?= values.map(x => s"${x.a}${x.b}").toVector)
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

      val sparkResult = ds.toDF()
        .select(untyped.concat_ws(",", $"a", $"b"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(concatWs(",", ds('a), ds('b)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("instr") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.nonEmptyListOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(x => X1(x + values.head)))

      val sparkResult = ds.toDF()
        .select(untyped.instr($"a", values.head))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds
        .select(instr(ds('a), values.head))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("length") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds.toDF()
        .select(untyped.length($"a"))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds
        .select(length(ds[String]('a)))
        .collect()
        .run()
        .toVector

      (typed ?= sparkResult).&&(values.map(_.a.length).toVector ?= typed)
    })
  }

  test("levenshtein") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds.toDF()
        .select(untyped.levenshtein($"a", untyped.concat($"a",untyped.lit("Hello"))))
        .map(_.getAs[Int](0))
        .collect()
        .toVector

      val typed = ds
        .select(levenshtein(ds('a), concat(ds('a),lit("Hello"))))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("regexp_replace") {
    val spark = session
    import spark.implicits._
    check(forAll { (values: List[X1[String]], n: Int) =>
      val ds = TypedDataset.create(values.map(x => X1(s"$n${x.a}-$n$n")))

      val sparkResult = ds.toDF()
        .select(untyped.regexp_replace($"a", "\\d+", "n"))
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

      val sparkResult = ds.toDF()
        .select(untyped.reverse($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(reverse(ds[String]('a)))
        .collect()
        .run()
        .toVector

      (typed ?= sparkResult).&&(values.map(_.a.reverse).toVector ?= typed)
    })
  }

  test("rpad") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds.toDF()
        .select(untyped.rpad($"a", 5, "hello"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(rpad(ds[String]('a), 5, "hello"))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("lpad") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds.toDF()
        .select(untyped.lpad($"a", 5, "hello"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(lpad(ds[String]('a), 5, "hello"))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("rtrim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds.toDF()
        .select(untyped.rtrim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(rtrim(ds[String]('a)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("ltrim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds.toDF()
        .select(untyped.ltrim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(ltrim(ds[String]('a)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("substring") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values)

      val sparkResult = ds.toDF()
        .select(untyped.substring($"a", 5, 3))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(substring(ds[String]('a), 5, 3))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("trim") {
    val spark = session
    import spark.implicits._
    check(forAll { values: List[X1[String]] =>
      val ds = TypedDataset.create(values.map(x => X1(s"  ${x.a}    ")))

      val sparkResult = ds.toDF()
        .select(untyped.trim($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(trim(ds[String]('a)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("upper") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.listOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(X1(_)))

      val sparkResult = ds.toDF()
        .select(untyped.upper($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(upper(ds[String]('a)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }

  test("lower") {
    val spark = session
    import spark.implicits._
    check(forAll(Gen.listOf(Gen.alphaStr)) { values: List[String] =>
      val ds = TypedDataset.create(values.map(X1(_)))

      val sparkResult = ds.toDF()
        .select(untyped.lower($"a"))
        .map(_.getAs[String](0))
        .collect()
        .toVector

      val typed = ds
        .select(lower(ds[String]('a)))
        .collect()
        .run()
        .toVector

      typed ?= sparkResult
    })
  }
}
