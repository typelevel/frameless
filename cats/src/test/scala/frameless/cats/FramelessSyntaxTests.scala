package frameless
package cats

import _root_.cats.data.ReaderT
import _root_.cats.effect.{ IO, Sync }
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

    // We need this instance here because there is no cats.effect.Sync instance for ReaderT.
    // Hopefully the instance will be back before cats 1.0.0 and we'll be able to get rid of this.
    implicit val sync: Sync[ReaderT[IO, SparkSession, ?]] = new Sync[ReaderT[IO, SparkSession, ?]] {
      def suspend[A](thunk: => ReaderT[IO, SparkSession, A]) = thunk
      def pure[A](x: A): ReaderT[IO, SparkSession, A] = ReaderT.pure(x)
      def handleErrorWith[A](fa: ReaderT[IO, SparkSession, A])(f: Throwable => ReaderT[IO, SparkSession, A]): ReaderT[IO, SparkSession, A] =
        ReaderT(r => fa.run(r).handleErrorWith(e => f(e).run(r)))
      def raiseError[A](e: Throwable): ReaderT[IO, SparkSession, A] = ReaderT.liftF(IO.raiseError(e))
      def flatMap[A, B](fa: ReaderT[IO, SparkSession, A])(f: A => ReaderT[IO, SparkSession, B]): ReaderT[IO, SparkSession, B] = fa.flatMap(f)
      def tailRecM[A, B](a: A)(f: A => ReaderT[IO, SparkSession, Either[A, B]]): ReaderT[IO, SparkSession, B] =
        ReaderT.catsDataMonadForKleisli[IO, SparkSession].tailRecM(a)(f)
    }

    check {
      forAll { (k:String, v: String) =>
        val scopedKey = "frameless.tests." + k
        1.pure[ReaderT[IO, SparkSession, ?]].withLocalProperty(scopedKey,v).run(session).unsafeRunSync()
        sc.getLocalProperty(scopedKey) ?= v

        1.pure[ReaderT[IO, SparkSession, ?]].withGroupId(v).run(session).unsafeRunSync()
        sc.getLocalProperty("spark.jobGroup.id") ?= v

        1.pure[ReaderT[IO, SparkSession, ?]].withDescription(v).run(session).unsafeRunSync()
        sc.getLocalProperty("spark.job.description") ?= v
      }
    }
  }
}
