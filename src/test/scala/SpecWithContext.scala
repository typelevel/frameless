package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import scala.reflect.runtime.universe.TypeTag
import typedframe.{TypedFrame, FromTraversableNullable}
import typedframe.TypeableRow

trait SpecWithContext extends FunSuite with BeforeAndAfterAll {
  implicit var implicitContext: SQLContext = null
  
  override def beforeAll(): Unit = {
    if (implicitContext == null) {
      val conf = new SparkConf().set("spark.sql.testkey", "true")
      val sc = new SparkContext("local[2]", "test-sql-context", conf)
      this.implicitContext = new SQLContext(sc)
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (implicitContext != null) {
        implicitContext.sparkContext.stop()
        implicitContext = null
      }
    } finally {
      super.afterAll()
    }
  }
  
  def checkAnswer[A <: Product](tf: TypedFrame[A], seq: Seq[A])(implicit t: TypeableRow[A]): Unit =
    assert(tf.collect() == seq)
  
  def checkAnswer[A <: Product](tf: TypedFrame[A], set: Set[A])(implicit t: TypeableRow[A]): Unit =
    assert(tf.collect().toSet == set)
  
  object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = implicitContext
  }
}
