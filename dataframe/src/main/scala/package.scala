import shapeless._
import org.apache.spark.sql.DataFrame

package object frameless {
  // Workaround until this commit is published:
  // https://github.com/milessabin/shapeless/commit/9ccbba8793a01a11cfea592c843d3deb465b33f9
  implicit val anyTypeable: Typeable[Any] = Typeable.anyTypeable

  implicit class DataFrameToTypedDataFrame(df: DataFrame) {
    def toTF[Schema <: Product](implicit fields: Fields[Schema]): TypedDataFrame[Schema] =
      new TypedDataFrame[Schema](df)
  }
}
