package frameless

import org.apache.spark.SparkContext

import _root_.cats.Id
import _root_.cats.data.Kleisli

package object cats {
  type SparkTask[A] = Kleisli[Id, SparkContext, A]
}
