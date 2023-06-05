package frameless.optimiser

import frameless._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

trait PushDownTests extends Matchers {

  implicit def session: SparkSession

  import Job.framelessSparkDelayForJob

  def withoutOptimisation[A]( thunk : => A): A
  def withOptimisation[A]( thunk : => A): A

  def gteTest[A: TypedEncoder : CatalystOrdered](payload: A, expected: Any) = {
    withoutOptimisation {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet("./target/optimiserTestData")
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet("./target/optimiserTestData"))

      val pushDowns = getPushDowns(dataset.filter(dataset('a) >= payload))

      pushDowns should not contain (GreaterThanOrEqual("a", expected))
    }

    withOptimisation {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet("./target/optimiserTestData")
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet("./target/optimiserTestData"))

      val ds = dataset.filter(dataset('a) >= payload)
      //ds.explain(true)
      val pushDowns = getPushDowns(ds)
      // prove the push down worked
      pushDowns should contain(GreaterThanOrEqual("a", expected))

      val collected = ds.collect().run.toVector.head
      // prove the serde isn't affected
      collected should be(X1(payload))
    }
  }

  def getPushDowns(dataset: TypedDataset[_]): Seq[Filter] = {
    val sparkPlan = dataset.queryExecution.executedPlan

    (if (sparkPlan.children.isEmpty)
    // assume it's AQE
      sparkPlan match {
        case aq: AdaptiveSparkPlanExec => aq.initialPlan
        case _ => sparkPlan
      }
    else
      sparkPlan).collect {
      case fs: FileSourceScanExec =>
        val m = fs.getClass.getMethod("pushedDownFilters", Array.empty[Class[_]]: _*)
        m.setAccessible(true)
        val res = m.invoke(fs).asInstanceOf[Seq[Filter]]
        res
    }.flatten
  }

}

trait TheTests extends AnyFunSuite with PushDownTests {

  test("sqlTimestamp pushdown") {
    val now = currentTimestamp()
    val sqlts = java.sql.Timestamp.from(microsToInstant(now))
    val ts = SQLTimestamp(now)
    val expected = sqlts

    gteTest(ts, expected)
  }

  test("instant pushdown") {
    val payload = Instant.now()
    val expected = java.sql.Timestamp.from(payload)

    gteTest(payload, expected)
  }
}

class ExperimentalLitTests extends TypedDatasetSuite with TheTests {
  def withoutOptimisation[A]( thunk : => A) = thunk

  def withOptimisation[A](thunk: => A): A = {
    val orig = session.sqlContext.experimental.extraOptimizations
    try {
      session.sqlContext.experimental.extraOptimizations ++= Seq(LiteralRule)

      thunk
    } finally {
      session.sqlContext.experimental.extraOptimizations = orig
    }
  }

}

class ExtensionLitTests extends TypedDatasetSuite with TheTests {

  var s: SparkSession = null

  override implicit def session: SparkSession = s

  def withoutOptimisation[A]( thunk : => A): A =
    try {
      s = SparkSession.builder().config(conf).getOrCreate()

      thunk
    } finally {
      stopSpark()
    }

  def withOptimisation[A](thunk: => A): A =
    try {
      s = SparkSession.builder().config(
        conf.clone().set("spark.sql.extensions", classOf[FramelessExtension].getName)
      ).getOrCreate()

      thunk
    } finally {
      stopSpark()
    }

  def stopSpark(): Unit =
    if (s != null) {
      s.stop()
      s = null
    }

  override def beforeAll(): Unit =
    stopSpark()

  override def afterAll(): Unit =
    stopSpark()

}
