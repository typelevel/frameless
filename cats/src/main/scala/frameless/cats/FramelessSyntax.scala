package frameless
package cats

import _root_.cats.effect.Sync
import _root_.cats.implicits._
import _root_.cats.mtl.ApplicativeAsk
import org.apache.spark.sql.SparkSession

trait FramelessSyntax extends frameless.FramelessSyntax {
  implicit class SparkJobOps[F[_], A](fa: F[A])(implicit S: Sync[F], A: ApplicativeAsk[F, SparkSession]) {
    import S._, A._

    def withLocalProperty(key: String, value: String): F[A] =
      for {
        session <- ask
        _       <- delay(session.sparkContext.setLocalProperty(key, value))
        a       <- fa
      } yield a

    def withGroupId(groupId: String): F[A] = withLocalProperty("spark.jobGroup.id", groupId)

    def withDescription(description: String) = withLocalProperty("spark.job.description", description)
  }
}
