package frameless

import org.apache.spark.sql.SparkSession

trait SparkDelay[F[_]] {

  def delay[A](
      a: => A
    )(implicit
      spark: SparkSession
    ): F[A]
}
