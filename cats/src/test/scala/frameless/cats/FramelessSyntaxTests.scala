package frameless
package cats

import _root_.cats.data.ReaderT
import _root_.cats.effect.IO
import _root_.cats.effect.unsafe.implicits.global
import frameless.{ TypedDataset, TypedDatasetSuite, TypedEncoder, X2 }
import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop, Prop._

class FramelessSyntaxTests extends TypedDatasetSuite {
  override val sparkDelay = null

  def prop[A, B](data: Vector[X2[A, B]])(
    implicit ev: TypedEncoder[X2[A, B]]
  ): Prop = {
    import implicits._

    val dataset = TypedDataset.create(data).dataset
    val dataframe = dataset.toDF()

    val typedDataset = dataset.typed
    val typedDatasetFromDataFrame = dataframe.unsafeTyped[X2[A, B]]

    typedDataset.collect[IO]().unsafeRunSync().toVector ?= typedDatasetFromDataFrame.collect[IO]().unsafeRunSync().toVector
  }

  test("dataset typed - toTyped") {
    check(forAll(prop[Int, String] _))
  }

  test("properties can be read back") {
    import implicits._
    import _root_.cats.implicits._
    import _root_.cats.mtl.implicits._

    check {
      forAll { (k:String, v: String) =>
        val scopedKey = "frameless.tests." + k
        1.pure[ReaderT[IO, SparkSession, *]].withLocalProperty(scopedKey,v).run(session).unsafeRunSync()
        sc.getLocalProperty(scopedKey) ?= v

        1.pure[ReaderT[IO, SparkSession, *]].withGroupId(v).run(session).unsafeRunSync()
        sc.getLocalProperty("spark.jobGroup.id") ?= v

        1.pure[ReaderT[IO, SparkSession, *]].withDescription(v).run(session).unsafeRunSync()
        sc.getLocalProperty("spark.job.description") ?= v
      }
    }
  }
}
