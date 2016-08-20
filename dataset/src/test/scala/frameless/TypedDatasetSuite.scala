package frameless

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class TypedDatasetSuite extends FunSuite with Checkers with SharedSparkContext {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 10, minSuccessful = 10)

  implicit def _sc = sc
  implicit def sqlContext = SparkSession.builder().getOrCreate().sqlContext
}
