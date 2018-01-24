package frameless

import java.time.Instant

import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.scalacheck.Prop._

import scala.math.Ordering.Implicits._

class ColumnTests extends TypedDatasetSuite {

  test("select('a < 'b, 'a <= 'b, 'a > 'b, 'a >= 'b)") {
    def prop[A: TypedEncoder : frameless.CatalystOrdered : scala.math.Ordering](a: A, b: A): Prop = {
      val dataset = TypedDataset.create(X2(a, b) :: Nil)
      val A = dataset.col('a)
      val B = dataset.col('b)

      val dataset2 = dataset.selectMany(
        A < B, A < b,   // One test uses columns, other uses literals
        A <= B, A <= b,
        A > B, A > b,
        A >= B, A >= b
      ).collect().run().toVector

      dataset2 ?= Vector((a < b, a < b, a <= b, a <= b, a > b, a > b, a >= b, a >= b))
    }

    implicit val sqlDateOrdering: Ordering[SQLDate] = Ordering.by(_.days)
    implicit val sqlTimestmapOrdering: Ordering[SQLTimestamp] = Ordering.by(_.us)

    implicit val arbInstant: Arbitrary[Instant] = Arbitrary(
      Gen.chooseNum(0L, Instant.MAX.getEpochSecond)
        .map(Instant.ofEpochSecond))
    implicit val instantAsLongInjection: Injection[Instant, Long] =
      Injection(_.getEpochSecond, Instant.ofEpochSecond)

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

      /*
      Fails at runtime!!
       Cause: org.apache.spark.sql.AnalysisException:
       cannot resolve '(`a` < `b`)' due to data type mismatch: '(`a` < `b`)'
       requires (boolean or tinyint or smallint or int or bigint or float or double or decimal or timestamp or date or string or binary) type, not array<int>;;
       */
//    check(forAll(prop[List[Int]] _)) //Fails at runtime!!

    /*
    Fails test!!
      Comparing a null/None item with another results in null/None in spark.
      This is different than what `Ordering` does!!
     */
//    check(forAll(prop[Option[Int]] _))

  }

  test("toString") {
    val t = TypedDataset.create((1,2)::Nil)
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
    def prop[A: TypedEncoder, B: TypedEncoder](a: Seq[X2[A,B]]) = {
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

}
