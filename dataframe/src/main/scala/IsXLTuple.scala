package frameless

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/** Type class witnessing that a type a tuple, up to Tuple64. */
trait IsXLTuple[T]

object IsXLTuple {
  implicit def apply[T]: IsXLTuple[T] = macro IsXLTupleMacro.mk[T]
}
