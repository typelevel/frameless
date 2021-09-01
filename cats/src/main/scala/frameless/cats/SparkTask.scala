package frameless
package cats

import org.apache.spark.SparkContext

import _root_.cats.Id
import _root_.cats.data.Kleisli

object SparkTask {
  def apply[A](f: SparkContext => A): SparkTask[A] =
    Kleisli[Id, SparkContext, A](f)

  def pure[A](a: => A): SparkTask[A] =
    Kleisli[Id, SparkContext, A](_ => a)
}
