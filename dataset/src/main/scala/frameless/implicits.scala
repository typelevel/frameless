package frameless

object implicits {
  object widen {
    // frameless prefixed to avoid implicit name collision

    implicit def framelessByteToShort[T](col: TypedColumn[T, Byte]): TypedColumn[T, Short] = col.cast[Short]
    implicit def framelessByteToInt[T](col: TypedColumn[T, Byte]): TypedColumn[T, Int] = col.cast[Int]
    implicit def framelessByteToLong[T](col: TypedColumn[T, Byte]): TypedColumn[T, Long] = col.cast[Long]
    implicit def framelessByteToDouble[T](col: TypedColumn[T, Byte]): TypedColumn[T, Double] = col.cast[Double]
    implicit def framelessByteToBigDecimal[T](col: TypedColumn[T, Byte]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]

    implicit def framelessShortToInt[T](col: TypedColumn[T, Short]): TypedColumn[T, Int] = col.cast[Int]
    implicit def framelessShortToLong[T](col: TypedColumn[T, Short]): TypedColumn[T, Long] = col.cast[Long]
    implicit def framelessShortToDouble[T](col: TypedColumn[T, Short]): TypedColumn[T, Double] = col.cast[Double]
    implicit def framelessShortToBigDecimal[T](col: TypedColumn[T, Short]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]

    implicit def framelessIntToLong[T](col: TypedColumn[T, Int]): TypedColumn[T, Long] = col.cast[Long]
    implicit def framelessIntToDouble[T](col: TypedColumn[T, Int]): TypedColumn[T, Double] = col.cast[Double]
    implicit def framelessIntToBigDecimal[T](col: TypedColumn[T, Int]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]

    implicit def framelessLongToDouble[T](col: TypedColumn[T, Long]): TypedColumn[T, Double] = col.cast[Double]
    implicit def framelessLongToBigDecimal[T](col: TypedColumn[T, Long]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]

    implicit def framelessDoubleToBigDecimal[T](col: TypedColumn[T, Double]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]

    // we don't have floats yet, but then this is lawful (or not?):
    //
    // implicit def byteToFloat[T](col: TypedColumn[T, Byte]): TypedColumn[T, Float] = col.cast[Float]
    // implicit def intToFloat[T](col: TypedColumn[T, Int]): TypedColumn[T, Float] = col.cast[Float]
    // implicit def longToFloat[T](col: TypedColumn[T, Long]): TypedColumn[T, Float] = col.cast[Float]
    // implicit def floatToDouble[T](col: TypedColumn[T, Float]): TypedColumn[T, Double] = col.cast[Double]
    // implicit def floatToBigDecimal[T](col: TypedColumn[T, Float]): TypedColumn[T, BigDecimal] = col.cast[BigDecimal]
  }

  object injections {
    implicit val javaBoolean: Injection[java.lang.Boolean, Boolean] = Injection(a => a, b => b)
    implicit val javaInt: Injection[java.lang.Integer, Int] = Injection(a => a, b => b)
    implicit val javaDouble: Injection[java.lang.Double, Double] = Injection(a => a, b => b)
    implicit val javaFloat: Injection[java.lang.Float, Float] = Injection(a => a, b => b)
    implicit val javaLong: Injection[java.lang.Long, Long] = Injection(a => a, b => b)
  }
}
