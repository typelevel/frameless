package frameless
package ops

import shapeless.test.illTyped
import shapeless.{::, Generic, HNil}

private case class XString(s1: String, s2: String, s3: String)
private case class XLong(l1: Long, l2: Long, l3: Long)

class MatchesAllTest extends TypedDatasetSuite {
  test("from HList") {
    type X = Int :: Int :: HNil
    implicitly[MatchesAll[X, Int]]

    type X2 = String :: String :: String :: String :: HNil
    implicitly[MatchesAll[X2, String]]

    illTyped("""implicitly[MatchesAll[X, String]]""")
  }

  test("from case class") {
    val xStringGen = Generic[XString]
    implicitly[MatchesAll[xStringGen.Repr, String]]

    val xLongGen = Generic[XLong]
    implicitly[MatchesAll[xLongGen.Repr, Long]]

    illTyped("""implicitly[MatchesAll[xLongGen.Repr, Int]]""")
  }
}
