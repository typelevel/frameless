package frameless
package ml

import org.scalactic.anyvals.PosZInt
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.prop.Checkers

class FramelessMlSuite extends FunSuite with Checkers with BeforeAndAfterAll with SparkTesting {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(sizeRange = PosZInt(10), minSize = PosZInt(10))
}
