package frameless

import scala.collection.immutable.Set

import org.scalatest.matchers.should.Matchers

object EncoderTests {
  case class Foo(s: Seq[(Int, Int)])
  case class Bar(s: Set[(Int, Int)])
  case class InstantRow(i: java.time.Instant)
  case class DurationRow(d: java.time.Duration)
  case class PeriodRow(p: java.time.Period)
}

class EncoderTests extends TypedDatasetSuite with Matchers {
  import EncoderTests._

  test("It should encode deeply nested collections") {
    implicitly[TypedEncoder[Seq[Foo]]]
    implicitly[TypedEncoder[Seq[Bar]]]
    implicitly[TypedEncoder[Set[Foo]]]
  }

  test("It should encode java.time.Instant") {
    implicitly[TypedEncoder[InstantRow]]
  }

  test("It should encode java.time.Duration") {
    implicitly[TypedEncoder[DurationRow]]
  }

  test("It should encode java.time.Period") {
    implicitly[TypedEncoder[PeriodRow]]
  }
}
