package frameless

import shapeless.Refute
import shapeless.test.illTyped

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class IsValueClassTests extends AnyFunSuite with Matchers {
  test("Case class is not Value class") {
    illTyped("IsValueClass[P]")
    illTyped("IsValueClass[Q]")
  }

  test("Scala value type is not Value class (excluded)") {
    illTyped("implicitly[IsValueClass[Double]]")
    illTyped("implicitly[IsValueClass[Float]]")
    illTyped("implicitly[IsValueClass[Long]]")
    illTyped("implicitly[IsValueClass[Int]]")
    illTyped("implicitly[IsValueClass[Char]]")
    illTyped("implicitly[IsValueClass[Short]]")
    illTyped("implicitly[IsValueClass[Byte]]")
    illTyped("implicitly[IsValueClass[Unit]]")
    illTyped("implicitly[IsValueClass[Boolean]]")
    illTyped("implicitly[IsValueClass[BigDecimal]]")
  }

  test("Value class evidence") {
    implicitly[IsValueClass[RecordEncoderTests.Name]]
    illTyped("implicitly[Refute[IsValueClass[RecordEncoderTests.Name]]]")
  }
}
