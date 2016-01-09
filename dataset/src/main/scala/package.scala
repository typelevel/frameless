import scala.language.implicitConversions
import org.apache.spark.sql._

package object frameless {
  implicit def enrichDataset[T](ds: Dataset[T]): RichDataset[T] = new RichDataset(ds)
}
