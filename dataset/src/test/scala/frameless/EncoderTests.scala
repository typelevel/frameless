package frameless

import org.scalatest.Matchers

object EncoderTests {
  case class Foo(s: Seq[(Int, Int)])
  case class Bar(s: Set[(Int, Int)])
}

class EncoderTests extends TypedDatasetSuite with Matchers {
  import EncoderTests._

  test("It should encode deeply nested collections") {
    implicitly[TypedEncoder[Seq[Foo]]]
  }

  test("It should encode deeply nested Set") {
    implicitly[TypedEncoder[Set[Bar]]]
  }
}
