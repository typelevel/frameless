package frameless

import java.time.Instant

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop}, Arbitrary.arbitrary
import org.scalatest.matchers.should.Matchers
import shapeless.test.illTyped
import ceedubs.irrec.regex.gen.CharRegexGen.genCharRegexAndCandidate

import scala.math.Ordering.Implicits._

final class ColumnTests extends TypedDatasetSuite with Matchers {

  private implicit object OrderingImplicits {
    implicit val sqlDateOrdering: Ordering[SQLDate] = Ordering.by(_.days)
    implicit val sqlTimestmapOrdering: Ordering[SQLTimestamp] = Ordering.by(_.us)
    implicit val arbInstant: Arbitrary[Instant] = Arbitrary(
      Gen.chooseNum(0L, Instant.MAX.getEpochSecond)
        .map(Instant.ofEpochSecond))
    implicit val instantAsLongInjection: Injection[Instant, Long] =
      Injection(_.getEpochSecond, Instant.ofEpochSecond)
  }

  test("select('a < 'b, 'a <= 'b, 'a > 'b, 'a >= 'b)") {
    import OrderingImplicits._
    def prop[A: TypedEncoder : CatalystOrdered : Ordering](a: A, b: A): Prop = {
      val dataset = TypedDataset.create(X2(a, b) :: Nil)
      val A = dataset.col('a)
      val B = dataset.col('b)

      val dataset2 = dataset.selectMany(
        A < B, A < b, // One test uses columns, other uses literals
        A <= B, A <= b,
        A > B, A > b,
        A >= B, A >= b
      ).collect().run().toVector

      dataset2 ?= Vector((a < b, a < b, a <= b, a <= b, a > b, a > b, a >= b, a >= b))
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Instant] _))
  }

  test("between") {
    import OrderingImplicits._
    def prop[A: TypedEncoder : CatalystOrdered : Ordering](a: A, b: A, c: A): Prop = {
      val dataset = TypedDataset.create(X3(a, b, c) :: Nil)
      val A = dataset.col('a)
      val B = dataset.col('b)
      val C = dataset.col('c)

      val isBetweeen = dataset.selectMany(A.between(B, C), A.between(b, c)).collect().run().toVector
      val result = b <= a && a <= c

      isBetweeen ?= Vector((result, result))
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Instant] _))
  }

  test("toString") {
    val t = TypedDataset.create((1, 2) :: Nil)
    t('_1).toString ?= t.dataset.col("_1").toString()
  }

  test("boolean and / or") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (s: Seq[X3[Boolean, Boolean, Boolean]]) =>
        val ds = TypedDataset.create(s)

        val typedBoolean = ds.select(
          ds('a) && ds('b) || ds('c),
          ds('a).and(ds('b)).or(ds('c))
        ).collect().run().toList

        val untypedDs = ds.toDF()
        val untypedBoolean = untypedDs.select(
          untypedDs("a") && untypedDs("b") || untypedDs("c"),
          untypedDs("a").and(untypedDs("b")).or(untypedDs("c"))
        ).as[(Boolean, Boolean)].collect().toList

        typedBoolean ?= untypedBoolean
      }
    }
  }

  test("substr") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (a: String, b: Int, c: Int) =>
        val ds = TypedDataset.create(X3(a, b, c) :: Nil)

        val typedSubstr = ds
          .select(ds('a).substr(ds('b), ds('c)))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedSubstr = untypedDs
          .select(untypedDs("a").substr(untypedDs("b"), untypedDs("c")))
          .as[String]
          .collect()
          .toList

        typedSubstr ?= untypedSubstr
      }
    }

    check {
      forAll { (a: String, b: Int, c: Int) =>
        val ds = TypedDataset.create(X1(a) :: Nil)

        val typedSubstr = ds
          .select(ds('a).substr(b, c))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedSubstr = untypedDs
          .select(untypedDs("a").substr(b, c))
          .as[String]
          .collect()
          .toList

        typedSubstr ?= untypedSubstr
      }
    }

    val ds1 = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds1.select(ds1('_1).substr(0, 5))""")
    illTyped("""ds1.select(ds1('_2).substr(0, 5))""")
    illTyped("""ds1.select(ds1('_3).substr(0, 5))""")
    illTyped("""ds1.select(ds1('_1).substr(ds1('_2), ds1('_3)))""")
  }

  test("like") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (a: String, b: String) =>
        val ds = TypedDataset.create(X2(a, b) :: Nil)

        val typedLike = ds
          .select(ds('a).like(a), ds('b).like(a))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedLike = untypedDs
          .select(untypedDs("a").like(a), untypedDs("b").like(a))
          .as[(Boolean, Boolean)]
          .collect()
          .toList

        typedLike ?= untypedLike
      }
    }

    val ds = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds.select(ds('_1).like("foo"))""")
    illTyped("""ds.select(ds('_2).like("foo"))""")
    illTyped("""ds.select(ds('_3).like("foo"))""")
  }

  test("rlike") {
    val spark = session
    import spark.implicits._

    check {
      forAll(genCharRegexAndCandidate[Char], arbitrary[String]) { (r, b) =>
        val a = r.candidate.mkString
        val ds = TypedDataset.create(X2(a, b) :: Nil)

        val typedLike = ds
          .select(ds('a).rlike(r.r.pprint), ds('b).rlike(r.r.pprint), ds('a).rlike(".*"))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedLike = untypedDs
          .select(untypedDs("a").rlike(r.r.pprint), untypedDs("b").rlike(r.r.pprint), untypedDs("a").rlike(".*"))
          .as[(Boolean, Boolean, Boolean)]
          .collect()
          .toList

        (typedLike ?= untypedLike)
      }
    }

    val ds = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds.select(ds('_1).rlike("foo"))""")
    illTyped("""ds.select(ds('_2).rlike("foo"))""")
    illTyped("""ds.select(ds('_3).rlike("foo"))""")
  }

  test("contains") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (a: String, b: String) =>
        val ds = TypedDataset.create(X2(a, b) :: Nil)

        val typedContains = ds
          .select(ds('a).contains(ds('b)), ds('b).contains(a))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedContains = untypedDs
          .select(untypedDs("a").contains(untypedDs("b")), untypedDs("b").contains(a))
          .as[(Boolean, Boolean)]
          .collect()
          .toList

        typedContains ?= untypedContains
      }
    }

    val ds = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds.select(ds('_1).contains("foo"))""")
    illTyped("""ds.select(ds('_2).contains("foo"))""")
    illTyped("""ds.select(ds('_3).contains("foo"))""")
  }

  test("startsWith") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (a: String, b: String) =>
        val ds = TypedDataset.create(X2(a, b) :: Nil)

        val typedStartsWith = ds
          .select(ds('a).startsWith(ds('b)), ds('b).startsWith(a))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedStartsWith = untypedDs
          .select(untypedDs("a").startsWith(untypedDs("b")), untypedDs("b").startsWith(a))
          .as[(Boolean, Boolean)]
          .collect()
          .toList

        typedStartsWith ?= untypedStartsWith
      }
    }

    val ds = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds.select(ds('_1).startsWith("foo"))""")
    illTyped("""ds.select(ds('_2).startsWith("foo"))""")
    illTyped("""ds.select(ds('_3).startsWith("foo"))""")
  }

  test("endsWith") {
    val spark = session
    import spark.implicits._

    check {
      forAll { (a: String, b: String) =>
        val ds = TypedDataset.create(X2(a, b) :: Nil)
        val typedStartsWith = ds
          .select(ds('a).endsWith(ds('b)), ds('b).endsWith(a))
          .collect()
          .run()
          .toList

        val untypedDs = ds.toDF()
        val untypedStartsWith = untypedDs
          .select(untypedDs("a").endsWith(untypedDs("b")), untypedDs("b").endsWith(a))
          .as[(Boolean, Boolean)]
          .collect()
          .toList

        typedStartsWith ?= untypedStartsWith
      }
    }

    val ds = TypedDataset.create((1, false, 2.0) :: Nil)
    illTyped("""ds.select(ds('_1).endsWith("foo"))""")
    illTyped("""ds.select(ds('_2).endsWith("foo"))""")
    illTyped("""ds.select(ds('_3).endsWith("foo"))""")
  }

  test("getOrElse") {
    def prop[A: TypedEncoder](a: A, opt: Option[A]) = {
      val dataset = TypedDataset.create(X2(a, opt) :: Nil)

      val defaulted: (A, A) = dataset
        .select(dataset('b).getOrElse(dataset('a)), dataset('b).getOrElse(a))
        .collect()
        .run()
        .toList
        .head

      defaulted.?=((opt.getOrElse(a), opt.getOrElse(a)))
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
  }

  test("asCol") {
    def prop[A: TypedEncoder, B: TypedEncoder](a: Seq[X2[A, B]]) = {
      val ds: TypedDataset[X2[A, B]] = TypedDataset.create(a)

      val frameless: Seq[(A, X2[A, B], X2[A, B], X2[A, B], B)] =
        ds.select(ds('a), ds.asCol, ds.asCol, ds.asCol, ds('b)).collect().run()

      val scala: Seq[(A, X2[A, B], X2[A, B], X2[A, B], B)] =
        a.map(x => (x.a, x, x, x, x.b))

      scala ?= frameless
    }

    check(forAll(prop[Int, Option[Long]] _))
    check(forAll(prop[Vector[Char], Option[Boolean]] _))
    check(forAll(prop[Vector[Vector[String]], Vector[Vector[BigDecimal]]] _))
  }

  test("asCol single column TypedDatasets") {
    def prop[A: TypedEncoder](a: Seq[A]) = {
      val ds: TypedDataset[A] = TypedDataset.create(a)

      val frameless: Seq[(A, A, A)] =
        ds.select(ds.asCol, ds.asCol, ds.asCol).collect().run()

      val scala: Seq[(A, A, A)] =
        a.map(x => (x, x, x))

      scala ?= frameless
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Vector[Vector[String]]] _))
  }

  test("asCol with numeric operators") {
    def prop(a: Seq[Long]) = {
      val ds: TypedDataset[Long] = TypedDataset.create(a)
      val (first,second) = (2L,5L)
      val frameless: Seq[(Long, Long, Long)] =
        ds.select(ds.asCol, ds.asCol+first, ds.asCol*second).collect().run()

      val scala: Seq[(Long, Long, Long)] =
        a.map(x => (x, x+first, x*second))

      scala ?= frameless
    }

    check(forAll(prop _))
  }

  test("unary_!") {
    val ds = TypedDataset.create((true, false) :: Nil)

    val rs = ds.select(!ds('_1), !ds('_2)).collect().run().head
    val expected = (false, true)

    rs shouldEqual expected
  }

  test("unary_! with non-boolean columns should not compile") {
    val ds = TypedDataset.create((1, "a", 2.0) :: Nil)

    "ds.select(!ds('_1))" shouldNot typeCheck
    "ds.select(!ds('_2))" shouldNot typeCheck
    "ds.select(!ds('_3))" shouldNot typeCheck
  }

  test("opt") {
    val data = (Option(1L), Option(2L)) :: (None, None) :: Nil
    val ds = TypedDataset.create(data)
    val rs = ds.select(ds('_1).opt.map(_ * 2), ds('_1).opt.map(_ + 2)).collect().run()
    val expected = data.map { case (x, y) => (x.map(_ * 2), y.map(_ + 1)) }
    rs shouldEqual expected
  }

  test("opt compiles only for columns of type Option[_]") {
    val ds = TypedDataset.create((1, List(1,2,3)) :: Nil)
    "ds.select(ds('_1).opt.map(x => x))" shouldNot typeCheck
    "ds.select(ds('_2).opt.map(x => x))" shouldNot typeCheck
  }

  test("field") {
    val ds = TypedDataset.create((1, (2.3F, "a")) :: Nil)
    val rs = ds.select(ds('_2).field('_2)).collect().run()

    rs shouldEqual Seq("a")
  }

  test("field compiles only for valid field") {
    val ds = TypedDataset.create((1, (2.3F, "a")) :: Nil)

    "ds.select(ds('_2).field('_3))" shouldNot typeCheck
  }

  test("col through lambda") {
    case class MyClass1(a: Int, b: String, c: MyClass2)
    case class MyClass2(d: Long)

    val ds = TypedDataset.create(Seq(MyClass1(1, "2", MyClass2(3L)), MyClass1(4, "5", MyClass2(6L))))

    assert(ds.col(_.a).isInstanceOf[TypedColumn[MyClass1, Int]])
    assert(ds.col(_.b).isInstanceOf[TypedColumn[MyClass1, String]])
    assert(ds.col(_.c.d).isInstanceOf[TypedColumn[MyClass1, Long]])

    "ds.col(_.c.toString)" shouldNot typeCheck
    "ds.col(_.c.toInt)" shouldNot typeCheck
    "ds.col(x => java.lang.Math.abs(x.a))" shouldNot typeCheck

    // we should be able to block the following as well...
    "ds.col(_.a.toInt)" shouldNot typeCheck
  }
}
