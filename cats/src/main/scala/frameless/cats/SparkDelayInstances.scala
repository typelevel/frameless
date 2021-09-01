package frameless
package cats

import org.apache.spark.sql.SparkSession

import _root_.cats.effect.Sync

trait SparkDelayInstances {
  implicit def framelessCatsSparkDelayForSync[F[_]](implicit S: Sync[F]): SparkDelay[F] =
    new SparkDelay[F] {
      def delay[A](a: => A)(implicit spark: SparkSession): F[A] = S.delay(a)
    }
}
