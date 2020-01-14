package frameless

import org.scalatest.matchers.should.Matchers

object EncoderTests {
  case class Foo(s: Seq[(Int, Int)])
}

class EncoderTests extends TypedDatasetSuite with Matchers {
  import EncoderTests._

  test("It should encode deeply nested collections") {
    implicitly[TypedEncoder[Seq[Foo]]]
  }
}
