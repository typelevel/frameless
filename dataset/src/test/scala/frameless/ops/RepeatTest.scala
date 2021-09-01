package frameless
package ops

import shapeless.test.illTyped
import shapeless.{::, HNil, Nat}

class RepeatTest extends TypedDatasetSuite {
  test("summoning with implicitly") {
    implicitly[Repeat.Aux[Int :: Boolean :: HNil, Nat._1, Int :: Boolean :: HNil]]
    implicitly[
      Repeat.Aux[Int :: Boolean :: HNil, Nat._2, Int :: Boolean :: Int :: Boolean :: HNil]]
    implicitly[Repeat.Aux[
      Int :: Boolean :: HNil,
      Nat._3,
      Int :: Boolean :: Int :: Boolean :: Int :: Boolean :: HNil]]
    implicitly[Repeat.Aux[
      String :: HNil,
      Nat._5,
      String :: String :: String :: String :: String :: HNil]]
  }

  test("ill typed") {
    illTyped(
      """implicitly[Repeat.Aux[String::HNil, Nat._5, String::String::String::String::HNil]]""")
  }
}
