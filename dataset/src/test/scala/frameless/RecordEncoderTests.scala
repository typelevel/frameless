package frameless

import org.scalatest.Matchers
import shapeless.test.illTyped

case class UnitsOnly(a: Unit, b: Unit)

case class TupleWithUnits(u0: Unit, _1: Int, u1: Unit, u2: Unit, _2: String, u3: Unit)

object TupleWithUnits {
  def apply(_1: Int, _2: String): TupleWithUnits = TupleWithUnits((), _1, (), (), _2, ())
}

class RecordEncoderTests extends TypedDatasetSuite with Matchers {
  test("Unable to encode products made from units only") {
    illTyped("""TypedEncoder[UnitsOnly]""")
  }

  test("Representation skips units") {
    assert(TypedEncoder[(Int, String)].catalystRepr == TypedEncoder[TupleWithUnits].catalystRepr)
  }

  test("Serialization skips units") {
    val df = session.createDataFrame(Seq((1, "one"), (2, "two")))
    val ds = df.as[TupleWithUnits](TypedExpressionEncoder[TupleWithUnits])
    val tds = TypedDataset.create(Seq(TupleWithUnits(1, "one"), TupleWithUnits(2, "two")))

    df.collect shouldEqual tds.toDF.collect
    ds.collect.toSeq shouldEqual tds.collect.run
  }
}
