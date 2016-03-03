package frameless

import _root_.cats.Id
import _root_.cats.data.Kleisli
import org.apache.spark.SparkContext

package object cats {
  type SparkTask[A] = Kleisli[Id, SparkContext, A]
}
