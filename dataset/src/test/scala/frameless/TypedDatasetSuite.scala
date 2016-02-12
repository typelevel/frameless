package frameless

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class TypedDatasetSuite extends FunSuite with Checkers {
  def sparkConf = new SparkConf()
    .setAppName("frameless-tests")
    .setMaster("local[*]")
    .set("spark.sql.testkey", "true")

  implicit def sc = SparkContext.getOrCreate(sparkConf)
  implicit def sqlContext = SQLContext.getOrCreate(sc)
}
