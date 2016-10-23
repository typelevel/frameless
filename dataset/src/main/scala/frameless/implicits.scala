package frameless

object implicits {
  object widen {
    implicit def intToDouble[T](col: TypedColumn[T, Int]): TypedColumn[T, Double] = col.cast[Double]
  }
}
