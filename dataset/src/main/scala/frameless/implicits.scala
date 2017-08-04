package frameless

object implicits {
  object widen {
    // frameless prefixed to avoid implicit name collision

    implicit def framelessByteToShort[T](col: TypedColumn[Byte]): TypedColumn[Short] = col.cast[Short]
    implicit def framelessByteToInt[T](col: TypedColumn[Byte]): TypedColumn[Int] = col.cast[Int]
    implicit def framelessByteToLong[T](col: TypedColumn[Byte]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessByteToDouble[T](col: TypedColumn[Byte]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessByteToBigDecimal[T](col: TypedColumn[Byte]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessShortToInt[T](col: TypedColumn[Short]): TypedColumn[Int] = col.cast[Int]
    implicit def framelessShortToLong[T](col: TypedColumn[Short]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessShortToDouble[T](col: TypedColumn[Short]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessShortToBigDecimal[T](col: TypedColumn[Short]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessIntToLong[T](col: TypedColumn[Int]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessIntToDouble[T](col: TypedColumn[Int]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessIntToBigDecimal[T](col: TypedColumn[Int]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessLongToDouble[T](col: TypedColumn[Long]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessLongToBigDecimal[T](col: TypedColumn[Long]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessDoubleToBigDecimal[T](col: TypedColumn[Double]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    // we don't have floats yet, but then this is lawful (or not?):
    //
    // implicit def byteToFloat[T](col: TypedColumn[Byte]): TypedColumn[Float] = col.cast[Float]
    // implicit def intToFloat[T](col: TypedColumn[Int]): TypedColumn[Float] = col.cast[Float]
    // implicit def longToFloat[T](col: TypedColumn[Long]): TypedColumn[Float] = col.cast[Float]
    // implicit def floatToDouble[T](col: TypedColumn[Float]): TypedColumn[Double] = col.cast[Double]
    // implicit def floatToBigDecimal[T](col: TypedColumn[Float]): TypedColumn[BigDecimal] = col.cast[BigDecimal]
  }
}
