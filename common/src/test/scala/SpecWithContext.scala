package org.apache.spark.sql

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

trait SpecWithContext extends FunSuite with SharedSparkContext {
  implicit def implicitContext = new SQLContext(sc)

  object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = implicitContext
  }
}
