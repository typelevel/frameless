package frameless
package ml

import org.scalactic.anyvals.PosZInt
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class FramelessMlSuite extends FunSuite with Checkers with SparkTesting {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(sizeRange = PosZInt(10), minSize = PosZInt(10))
}
