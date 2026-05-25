package frameless
package cats

import _root_.cats.Id
import _root_.cats.data.Kleisli
import org.apache.spark.SparkContext

object SparkTask {

  def apply[A](f: SparkContext => A): SparkTask[A] =
    Kleisli[Id, SparkContext, A](f)

  def pure[A](a: => A): SparkTask[A] =
    Kleisli[Id, SparkContext, A](_ => a)
}
