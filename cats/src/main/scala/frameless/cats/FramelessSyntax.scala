package frameless
package cats

import _root_.cats.effect.Sync
import _root_.cats.syntax.all._
import _root_.cats.mtl.Ask
import org.apache.spark.sql.SparkSession

trait FramelessSyntax extends frameless.FramelessSyntax {

  implicit class SparkJobOps[F[_], A](
      fa: F[A]
    )(implicit
      S: Sync[F],
      A: Ask[F, SparkSession]) {
    import S._, A._

    def withLocalProperty(key: String, value: String): F[A] =
      for {
        session <- ask
        _ <- delay(session.sparkContext.setLocalProperty(key, value))
        a <- fa
      } yield a

    def withGroupId(groupId: String): F[A] =
      withLocalProperty("spark.jobGroup.id", groupId)

    def withDescription(description: String): F[A] =
      withLocalProperty("spark.job.description", description)
  }
}
