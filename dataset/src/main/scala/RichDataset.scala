package frameless

import org.apache.spark.sql._
import shapeless._

class RichDataset[T](ds: Dataset[T]) {
  /** Returns a new [[Dataset]] where each record has been mapped on to the specified type. */
  def typedAs[U] = new TypedAsPartial[U]
  
  // Emulate mutiple type parameter lists
  class TypedAsPartial[U] {
    def apply[S <: HList]()
      (implicit
        e: Encoder[U],
        t: Generic.Aux[T, S],
        u: Generic.Aux[U, S]
      ): Dataset[U] =
        ds.as[U]
  }
}
