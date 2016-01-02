import shapeless._

package object typedframe {
  implicit val TODO: Typeable[Any] = Typeable.anyTypeable
}
