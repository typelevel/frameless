package frameless
package cats

import _root_.cats.data.ReaderT
import _root_.cats.effect.IO
import _root_.cats.effect.unsafe.implicits.global
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalacheck.{Test => PTest}
import org.scalacheck.Prop, Prop._
import org.scalacheck.effect.PropF, PropF._

class FramelessSyntaxTests extends TypedDatasetSuite with Matchers {
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
    import _root_.cats.syntax.all._

    forAllF { (k: String, v: String) =>
      val scopedKey = "frameless.tests." + k
      1
        .pure[ReaderT[IO, SparkSession, *]]
        .withLocalProperty(scopedKey, v)
        .withGroupId(v)
        .withDescription(v)
        .run(session)
        .map { _ =>
          sc.getLocalProperty(scopedKey) shouldBe v
          sc.getLocalProperty("spark.jobGroup.id") shouldBe v
          sc.getLocalProperty("spark.job.description") shouldBe v
        }.void
    }.check().unsafeRunSync().status shouldBe PTest.Passed
  }
}
