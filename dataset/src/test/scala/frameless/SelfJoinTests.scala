package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.apache.spark.sql.{SparkSession, functions => sparkFunctions}

class SelfJoinTests extends TypedDatasetSuite {
  // Without crossJoin.enabled=true Spark doesn't like trivial join conditions:
  // [error] Join condition is missing or trivial.
  // [error] Use the CROSS JOIN syntax to allow cartesian products between these relations.
  def allowTrivialJoin[T](body: => T)(implicit session: SparkSession): T = {
    val crossJoin = "spark.sql.crossJoin.enabled"
    val oldSetting = session.conf.get(crossJoin)
    session.conf.set(crossJoin, "true")
    val result = body
    session.conf.set(crossJoin, oldSetting)
    result
  }

  def allowAmbiguousJoin[T](body: => T)(implicit session: SparkSession): T = {
    val crossJoin = "spark.sql.analyzer.failAmbiguousSelfJoin"
    val oldSetting = session.conf.get(crossJoin)
    session.conf.set(crossJoin, "false")
    val result = body
    session.conf.set(crossJoin, oldSetting)
    result
  }

  test("self join with colLeft/colRight disambiguation") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering
    ](dx: List[X2[A, B]], d: X2[A, B]): Prop = allowAmbiguousJoin {
      val data = d :: dx
      val ds = TypedDataset.create(data)

      // This is the way to write unambiguous self-join in vanilla, see https://goo.gl/XnkSUD
      val df1 = ds.dataset.as("df1")
      val df2 = ds.dataset.as("df2")
      val vanilla = df1.join(df2,
        sparkFunctions.col("df1.a") === sparkFunctions.col("df2.a")).count()

      val typed = ds.joinInner(ds)(
        ds.colLeft('a) === ds.colRight('a)
      ).count().run()

      vanilla ?= typed
    }

    check(prop[Int, Int] _)
  }

  test("trivial self join") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering
    ](dx: List[X2[A, B]], d: X2[A, B]): Prop =
      allowTrivialJoin { allowAmbiguousJoin {

        val data = d :: dx
        val ds = TypedDataset.create(data)
        val untyped = ds.dataset
        // Interestingly, even with aliasing it seems that it's impossible to
        // obtain a trivial join condition of shape df1.a == df1.a, Spark we
        // always interpret that as df1.a == df2.a. For the purpose of this
        // test we fall-back to lit(true) instead.
        // val trivial = sparkFunctions.col("df1.a") === sparkFunctions.col("df1.a")
        val trivial = sparkFunctions.lit(true)
        val vanilla = untyped.as("df1").join(untyped.as("df2"), trivial).count()

        val typed = ds.joinInner(ds)(ds.colLeft('a) === ds.colLeft('a)).count().run
        vanilla ?= typed
      } }

    check(prop[Int, Int] _)
  }

  test("self join with unambiguous expression") {
    def prop[
      A : TypedEncoder : CatalystNumeric : Ordering,
      B : TypedEncoder : Ordering
    ](data: List[X3[A, A, B]]): Prop = allowAmbiguousJoin {
      val ds = TypedDataset.create(data)

      val df1 = ds.dataset.alias("df1")
      val df2 = ds.dataset.alias("df2")

      val vanilla = df1.join(df2,
        (sparkFunctions.col("df1.a") + sparkFunctions.col("df1.b")) ===
        (sparkFunctions.col("df2.a") + sparkFunctions.col("df2.b"))).count()

      val typed = ds.joinInner(ds)(
        (ds.colLeft('a) + ds.colLeft('b)) === (ds.colRight('a) + ds.colRight('b))
      ).count().run()

      vanilla ?= typed
    }

    check(prop[Int, Int] _)
  }

  test("Do you want ambiguous self join? This is how you get ambiguous self join.") {
    def prop[
      A : TypedEncoder : CatalystNumeric : Ordering,
      B : TypedEncoder : Ordering
    ](data: List[X3[A, A, B]]): Prop =
      allowTrivialJoin { allowAmbiguousJoin {
        val ds = TypedDataset.create(data)

        // The point I'm making here is that it "behaves just like Spark". I
        // don't know (or really care about how) how Spark disambiguates that
        // internally...
        val vanilla = ds.dataset.join(ds.dataset,
          (ds.dataset("a") + ds.dataset("b")) ===
          (ds.dataset("a") + ds.dataset("b"))).count()

        val typed = ds.joinInner(ds)(
          (ds.col('a) + ds.col('b)) === (ds.col('a) + ds.col('b))
        ).count().run()

        vanilla ?= typed
      } }

      check(prop[Int, Int] _)
    }

  test("colLeft and colRight are equivalent to col outside of joins") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      ex4: TypedEncoder[X4[A, B, C, D]]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val selectedCol      = dataset.select(dataset.col     [A]('a)).collect().run().toVector
      val selectedColLeft  = dataset.select(dataset.colLeft [A]('a)).collect().run().toVector
      val selectedColRight = dataset.select(dataset.colRight[A]('a)).collect().run().toVector

      (selectedCol ?= selectedColLeft) && (selectedCol ?= selectedColRight)
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[X2[Int, Int], Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[UdtEncodedClass, Int, Int, Int] _))
  }

  test("colLeft and colRight are equivalent to col outside of joins - via files (codegen)") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      ex4: TypedEncoder[X4[A, B, C, D]]
    ): Prop = {
      TypedDataset.create(data).write.mode("overwrite").parquet("./target/testData")
      val dataset = TypedDataset.createUnsafe[X4[A, B, C, D]](session.read.parquet("./target/testData"))
      val selectedCol      = dataset.select(dataset.col     [A]('a)).collect().run().toVector
      val selectedColLeft  = dataset.select(dataset.colLeft [A]('a)).collect().run().toVector
      val selectedColRight = dataset.select(dataset.colRight[A]('a)).collect().run().toVector

      (selectedCol ?= selectedColLeft) && (selectedCol ?= selectedColRight)
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[X2[Int, Int], Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[UdtEncodedClass, Int, Int, Int] _))
  }
}
