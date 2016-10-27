package frameless

object implicits {
  object widen {
    // frameless prefixed to avoid implicit name collision

    implicit def framelessByteToShort(col: TypedColumn[Byte]): TypedColumn[Short] = col.cast[Short]
    implicit def framelessByteToInt(col: TypedColumn[Byte]): TypedColumn[Int] = col.cast[Int]
    implicit def framelessByteToLong(col: TypedColumn[Byte]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessByteToDouble(col: TypedColumn[Byte]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessByteToBigDecimal(col: TypedColumn[Byte]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessShortToInt(col: TypedColumn[Short]): TypedColumn[Int] = col.cast[Int]
    implicit def framelessShortToLong(col: TypedColumn[Short]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessShortToDouble(col: TypedColumn[Short]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessShortToBigDecimal(col: TypedColumn[Short]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessIntToLong(col: TypedColumn[Int]): TypedColumn[Long] = col.cast[Long]
    implicit def framelessIntToDouble(col: TypedColumn[Int]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessIntToBigDecimal(col: TypedColumn[Int]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessLongToDouble(col: TypedColumn[Long]): TypedColumn[Double] = col.cast[Double]
    implicit def framelessLongToBigDecimal(col: TypedColumn[Long]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    implicit def framelessDoubleToBigDecimal(col: TypedColumn[Double]): TypedColumn[BigDecimal] = col.cast[BigDecimal]

    // we don't have floats yet, but then this is lawful (or not?):
    //
    // implicit def byteToFloat(col: TypedColumn[Byte]): TypedColumn[Float] = col.cast[Float]
    // implicit def intToFloat(col: TypedColumn[Int]): TypedColumn[Float] = col.cast[Float]
    // implicit def longToFloat(col: TypedColumn[Long]): TypedColumn[Float] = col.cast[Float]
    // implicit def floatToDouble(col: TypedColumn[Float]): TypedColumn[Double] = col.cast[Double]
    // implicit def floatToBigDecimal(col: TypedColumn[Float]): TypedColumn[BigDecimal] = col.cast[BigDecimal]
  }
}
