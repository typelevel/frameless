import shapeless._
import org.apache.spark.sql.DataFrame

package object typedframe {
  // Workaround until this commit is published:
  // https://github.com/milessabin/shapeless/commit/9ccbba8793a01a11cfea592c843d3deb465b33f9
  implicit val anyTypeable: Typeable[Any] = Typeable.anyTypeable
  
  implicit class DataFrameToTypedFrame(df: DataFrame) {
    def toTF[Schema <: Product](implicit fields: Fields[Schema]): TypedFrame[Schema] =
      new TypedFrame[Schema](df)
  }
}
