package frameless.optimiser

import frameless._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, EqualTo}
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

  def gteTest[A: TypedEncoder : CatalystOrdered](payload: A, expected: Any, expectFailureWithExperimental: Boolean = false) =
    pushDownTest[A](payload, GreaterThanOrEqual("a", expected), _ >= payload, expectFailureWithExperimental)

  def eqTest[A: TypedEncoder : CatalystOrdered](payload: A, expected: Any, expectFailureWithExperimental: Boolean = false) =
    pushDownTest[A](payload, EqualTo("a", expected), _ === payload, expectFailureWithExperimental)

  val isExperimental: Boolean

  def pushDownTest[A: TypedEncoder : CatalystOrdered](payload: A, expected: Any, op: TypedColumn[X1[A],A] => TypedColumn[X1[A],Boolean], expectFailureWithExperimental: Boolean = false) = {
    withoutOptimisation {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet("./target/optimiserTestData")
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet("./target/optimiserTestData"))

      val pushDowns = getPushDowns(dataset.filter(op(dataset('a))))

      pushDowns should not contain (expected)
    }

    withOptimisation {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet("./target/optimiserTestData")
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet("./target/optimiserTestData"))

      val ds = dataset.filter(op(dataset('a) ))
      ds.explain(true)
      val pushDowns = getPushDowns(ds)

      // prove the push down worked when expected
      if (isExperimental && expectFailureWithExperimental)
        pushDowns should not contain(expected)
      else
        pushDowns should contain(expected)

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
        import scala.reflect.runtime.{universe => ru}

        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
        val instanceMirror = runtimeMirror.reflect(fs)
        val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
        val m = instanceMirror.reflectMethod(getter.asMethod)
        val res = m.apply(fs).asInstanceOf[Seq[Filter]]

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

  test("struct pushdown") {
    val payload = X1(X4(1,2,3,4))
    val expected = new GenericRowWithSchema(Array(Row(1,2,3,4)), implicitly[TypedEncoder[X1[X4[Int,Int,Int,Int]]]].catalystRepr.asInstanceOf[StructType])

    eqTest(payload, expected, expectFailureWithExperimental = true)
  }
}

class ExperimentalLitTests extends TypedDatasetSuite with TheTests {
  val isExperimental = true

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
  val isExperimental = false

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
