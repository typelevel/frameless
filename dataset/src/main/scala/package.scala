import scala.language.implicitConversions
import org.apache.spark.sql._

package object frameless {
  implicit def enrichDataset[Schema <: Product](ds: Dataset[Schema])
    (implicit fields: Fields[Schema]): RichDataset[Schema] =
      new RichDataset(ds)
}
