package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import scala.reflect.runtime.universe.TypeTag
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

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
  
  object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = implicitContext
  }
}
