package frameless

import org.scalacheck.Prop
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

      val defaulted = dataset
        .select(dataset('b).getOrElse(dataset('a)))
        .collect()
        .run
        .toList
        .head

      defaulted ?= opt.getOrElse(a)
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
}
