package frameless

import frameless.functions.lit

import org.scalatest.matchers.should.Matchers

import org.scalacheck.Prop, Prop._

import RecordEncoderTests.Name

class LitTests extends TypedDatasetSuite with Matchers {

  def prop[A: TypedEncoder](
      value: A
    )(implicit
      i0: shapeless.Refute[IsValueClass[A]]
    ): Prop = {
    val df: TypedDataset[Int] = TypedDataset.create(1 :: Nil)

    val l: TypedColumn[Int, A] = lit(value)

    // filter forces whole codegen
    val elems = df.deserialized
      .filter((_: Int) => true)
      .select(l)
      .collect()
      .run()
      .toVector

    // otherwise it uses local relation
    val localElems = df.select(l).collect().run().toVector

    val expected = Vector(value)

    (localElems ?= expected) && (elems ?= expected)
  }

  test("select(lit(...))") {
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)

    check(prop[Option[Int]] _)
    check(prop[Option[String]] _)

    check(prop[Vector[Long]] _)
    check(prop[Vector[X1[Long]]] _)

    check(prop[Vector[String]] _)
    check(prop[Vector[X1[String]]] _)

    check(prop[X1[Int]] _)
    check(prop[X1[X1[Int]]] _)

    check(prop[Food] _)

    // doesn't work, object has to be serializable
    // check(prop[frameless.LocalDateTime] _)
  }

  test("support value class") {
    val initial =
      Seq(Q(name = new Name("Foo"), id = 1), Q(name = new Name("Bar"), id = 2))
    val ds = TypedDataset.create(initial)

    ds.collect.run() shouldBe initial

    val lorem = new Name("Lorem")

    ds.withColumnReplaced('name, functions.litValue(lorem))
      .collect
      .run() shouldBe initial.map(_.copy(name = lorem))
  }

  test("support optional value class") {
    val initial = Seq(
      R(name = "Foo", id = 1, alias = None),
      R(name = "Bar", id = 2, alias = Some(new Name("Lorem")))
    )
    val ds = TypedDataset.create(initial)

    ds.collect.run() shouldBe initial

    val someIpsum: Option[Name] = Some(new Name("Ipsum"))

    val lit = functions.litValue(someIpsum)
    val tds = ds.withColumnReplaced('alias, functions.litValue(someIpsum))

    tds.queryExecution.toString() should include(lit.toString)

    tds.collect.run() shouldBe initial.map(_.copy(alias = someIpsum))

    ds.withColumnReplaced('alias, functions.litValue(Option.empty[Name]))
      .collect
      .run() shouldBe initial.map(_.copy(alias = None))
  }

  test("#205: comparing literals encoded using Injection") {
    import org.apache.spark.sql.catalyst.util.DateTimeUtils
    implicit val dateAsInt: Injection[java.sql.Date, Int] =
      Injection(DateTimeUtils.fromJavaDate, DateTimeUtils.toJavaDate)

    val today = new java.sql.Date(System.currentTimeMillis)
    val data = Vector(P(42, today))
    val tds = TypedDataset.create(data)

    tds.filter(tds('d) === today).collect.run().map(_.i) shouldBe Seq(42)
  }
}

final case class P(i: Int, d: java.sql.Date)

final case class Q(id: Int, name: Name)

final case class R(id: Int, name: String, alias: Option[Name])
