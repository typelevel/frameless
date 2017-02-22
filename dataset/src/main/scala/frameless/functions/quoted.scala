package frameless.functions

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag
import org.apache.spark.sql.catalyst.expressions.{Factorial, Hex}
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.{Column, functions => sf}

/**
  * Quoted functions, for being used inside an expression
  * Functions annotated with @sparkFunction are rewritten accordingly, whereas other functions are converted to UDFs and
  * actually applied.
  *
  * Note that these functions have simple implementations in order to provide explanation of what they do, and so that
  * it won't be catastrophic to call them in the wrong context. But the implementations ought to never be invoked
  * at run time.
  */
object quoted {

  // $COVERAGE-OFF$ - these functions will never actually be invoked at runtime

  sealed trait QuotedFunc
  class sparkFunction0[B](referrent: () => B) extends StaticAnnotation with QuotedFunc
  class sparkFunction[A, B](referent: A => B) extends StaticAnnotation with QuotedFunc
  class sparkColumnOp[A, B](referrent: Column => A => B) extends StaticAnnotation with QuotedFunc
  class sparkAggregate[A, B](referent: A => B) extends StaticAnnotation with QuotedFunc
  class sparkWindowFunction[A, B](referent: A => B) extends StaticAnnotation with QuotedFunc

  @sparkFunction(sf.abs)
  def abs[T : Numeric](t: T): T = implicitly[Numeric[T]].abs(t)

  @sparkFunction((cols: Seq[Column]) => sf.array(cols:_*))
  def array[T : ClassTag](items: T*): Array[T] = items.toArray

  @sparkFunction((cols: Seq[(Column,Column)]) => sf.map(cols.flatMap(a => Seq(a._1, a._2)):_*))
  def map[T, U](items: (T, U)*): Map[T, U] = items.toMap

  @sparkFunction((cols: Seq[Column]) => sf.coalesce(cols:_*))
  def coalesce[T](items: T*): T = items.find(_ != null).getOrElse(null.asInstanceOf[T])

  @sparkFunction0(sf.input_file_name)
  def input_file_name(): String = "invoked outside of quoted expression"

  @sparkFunction(sf.isnan)
  def isnan[T: Numeric](t: T): Boolean = implicitly[Numeric[T]].toDouble(t).isNaN

  @sparkFunction(sf.isnull)
  def isnull[T](t: T): Boolean = t == null

  private lazy val longs = Stream.range(0L, Long.MaxValue).iterator
  @sparkFunction0(sf.monotonically_increasing_id)
  def monotonically_increasing_id(): Long = longs.next()

  @sparkFunction((sf.nanvl _).tupled)
  def nanvl(first: Double, second: Double): Double = if(java.lang.Double.isNaN(first)) second else first

  @sparkFunction((sf.nanvl _).tupled)
  def nanvl(first: Float, second: Float): Float = if(java.lang.Float.isNaN(first)) second else first

  @sparkFunction(sf.negate)
  def negate[T : Numeric](t: T): T = implicitly[Numeric[T]].negate(t)

  @sparkFunction(sf.not)
  def not(b: Boolean): Boolean = !b

  @sparkFunction(sf.rand)
  def rand(seed: Long): Double = {
    scala.util.Random.setSeed(seed)
    scala.util.Random.nextDouble()
  }

  @sparkFunction0(sf.rand)
  def rand(): Double = scala.util.Random.nextDouble()

  @sparkFunction(sf.randn)
  def randn(seed: Long): Double = {
    scala.util.Random.setSeed(seed)
    scala.util.Random.nextGaussian()
  }

  @sparkFunction0(sf.randn)
  def randn(): Double = scala.util.Random.nextGaussian()

  @sparkFunction0(sf.spark_partition_id)
  def spark_partition_id(): Int = -1

  @sparkFunction(sf.sqrt(_: Column))
  def sqrt[T](t: T)(implicit frac: Fractional[T]): Double = math.sqrt(frac.toDouble(t))


  case class CaseBuilder[T]() {
    @sparkColumnOp(col => (col.when _).tupled)
    def when(condition: Boolean, value: T): CaseBuilder[T] = this

    @sparkColumnOp(_.otherwise)
    def otherwise(value: T): T = value
  }

  @sparkFunction((sf.when _).tupled)
  def when[T](condition: Boolean, value: T): CaseBuilder[T] = CaseBuilder[T]()

  @sparkFunction(sf.bitwiseNOT)
  def bitwiseNOT(b: Byte): Byte = (~b).toByte

  @sparkFunction(sf.bitwiseNOT)
  def bitwiseNOT(s: Short): Short = (~s).toShort

  @sparkFunction(sf.bitwiseNOT)
  def bitwiseNOT(i: Int): Int = ~i

  @sparkFunction(sf.bitwiseNOT)
  def bitwiseNOT(l: Long): Long = ~l

  @sparkFunction(sf.acos(_:Column))
  def acos(d: Double): Double = scala.math.acos(d)

  @sparkFunction(sf.asin(_:Column))
  def asin(d: Double): Double = scala.math.asin(d)

  @sparkFunction(sf.atan(_:Column))
  def atan(d: Double): Double = scala.math.atan(d)

  @sparkFunction((sf.atan2(_: Column, _: Column)).tupled)
  def atan2(l: Double, r: Double): Double = scala.math.atan2(l, r)

  @sparkFunction(sf.bin(_:Column))
  def bin(l: Long): String = java.lang.Long.toBinaryString(l)

  @sparkFunction(sf.cbrt(_:Column))
  def cbrt(d: Double): Double = math.cbrt(d)

  @sparkFunction(sf.ceil(_:Column))
  def ceil(d: Double): Double = math.ceil(d)

  @sparkFunction((sf.conv _).tupled)
  def conv(num: String, fromBase: Int, toBase: Int): String =
    NumberConverter.convert(num.getBytes(), fromBase, toBase).toString

  @sparkFunction(sf.cos(_:Column))
  def cos(n: Double): Double = math.cos(n)

  @sparkFunction(sf.cosh(_:Column))
  def cosh(n: Double): Double = math.cosh(n)

  @sparkFunction(sf.exp(_:Column))
  def exp(n: Double): Double = math.exp(n)

  @sparkFunction(sf.expm1(_:Column))
  def expm1(n: Double): Double = math.expm1(n)

  @sparkFunction(sf.factorial)
  def factorial(n: Int): Long = Factorial.factorial(n)

  @sparkFunction(sf.floor(_:Column))
  def floor(d: Double): Double = math.floor(d)

  @sparkFunction((cols: Seq[Column]) => sf.greatest(cols:_*))
  def greatest[T : Ordering](first: T, rest: T*): T = rest.foldLeft(first)(implicitly[Ordering[T]].max)

  @sparkFunction(sf.hex)
  def hex(arr: Array[Byte]): String = Hex.hex(arr).toString

  @sparkFunction(sf.hex)
  def hex(l: Long): String = Hex.hex(l).toString

  @sparkFunction(sf.unhex)
  def unhex(str: String): Array[Byte] = Hex.unhex(str.getBytes())

  @sparkFunction((sf.hypot(_:Column,_:Column)).tupled)
  def hypot(a: Double, b: Double): Double = math.hypot(a, b)

  @sparkFunction((cols: Seq[Column]) => sf.least(cols:_*))
  def least[T : Ordering](first: T, rest: T*) = rest.foldLeft(first)(implicitly[Ordering[T]].min)

  @sparkFunction(sf.log(_:Column))
  def log(n: Double): Double = math.log(n)

  @sparkFunction((sf.log(_:Double,_:Column)).tupled)
  def log(base: Double, n: Double): Double = math.log(n) / math.log(base)

  @sparkFunction(sf.log10(_:Column))
  def log10(n: Double): Double = math.log10(n)

  @sparkFunction(sf.log1p(_:Column))
  def log1p(n: Double): Double = math.log1p(n)

  @sparkFunction(sf.log2(_:Column))
  def log2(n: Double): Double = log(n, 2.0)

  @sparkFunction((sf.pow(_:Column,_:Column)).tupled)
  def pow(base: Double, exp: Double): Double = math.pow(base, exp)

  @sparkFunction((sf.pmod _).tupled)
  def pmod[T](dividend: T, divisor: T)(implicit int: Integral[T]): T = {
    val rem = int.rem(dividend, divisor)
    if(int.lt(rem, int.zero))
      int.plus(rem, divisor)
    else
      rem
  }

  @sparkFunction(sf.rint(_:Column))
  def rint(n: Double): Double = math.rint(n)

  @sparkFunction(sf.round(_:Column))
  def round(n: Double): Long = math.round(n)

  @sparkFunction(sf.round(_:Column))
  def round(n: Float): Int = math.round(n)

  @sparkFunction((sf.round(_:Column, _:Int)).tupled)
  def round(n: Double, scale: Int): Double = BigDecimal(n).setScale(scale, RoundingMode.HALF_UP).toDouble

  @sparkFunction(sf.bround(_:Column))
  def bround(n: Double): Long = BigDecimal(n).setScale(0, RoundingMode.HALF_EVEN).toLong

  @sparkFunction((sf.bround(_:Column, _:Int)).tupled)
  def bround(n: Double, scale: Int): Double = BigDecimal(n).setScale(scale, RoundingMode.HALF_EVEN).toDouble

  @sparkFunction((sf.shiftLeft _).tupled)
  def shiftLeft(l: Long, bits: Int): Long = l << bits

  @sparkFunction((sf.shiftLeft _).tupled)
  def shiftLeft(i: Int, bits: Int): Int = i << bits

  @sparkFunction((sf.shiftRight _).tupled)
  def shiftRight(l: Long, bits: Int): Long = l >> bits

  @sparkFunction((sf.shiftRight _).tupled)
  def shiftRight(i: Int, bits: Int): Int = i >> bits

  @sparkFunction((sf.shiftRightUnsigned _).tupled)
  def shiftRightUnsigned(l: Long, bits: Int): Long = l >>> bits

  @sparkFunction((sf.shiftRightUnsigned _).tupled)
  def shiftRightUnsigned(i: Int, bits: Int): Int = i >>> bits

  @sparkFunction(sf.signum(_:Column))
  def signum[T : Numeric](t: T): Int = implicitly[Numeric[T]].signum(t)

  @sparkFunction(sf.sin(_:Column))
  def sin(d: Double): Double = math.sin(d)

  @sparkFunction(sf.sinh(_:Column))
  def sinh(d: Double): Double = math.sinh(d)

  @sparkFunction(sf.tan(_:Column))
  def tan(d: Double): Double = math.tan(d)

  @sparkFunction(sf.tanh(_:Column))
  def tanh(d: Double): Double = math.tanh(d)

  @sparkFunction(sf.toDegrees(_:Column))
  def toDegrees(d: Double): Double = math.toDegrees(d)

  @sparkFunction(sf.toRadians(_:Column))
  def toRadians(d: Double): Double = math.toRadians(d)

  /////////////////////////
  // Aggregate functions //
  /////////////////////////
  private val aggMsg = "Aggregate function can only be used inside a TypedDataset expression"

  @sparkAggregate(sf.approxCountDistinct(_:Column))
  @compileTimeOnly(aggMsg)
  def approxCountDistinct[T](t: T): Long = ???

  @sparkAggregate((sf.approxCountDistinct(_:Column,_:Double)).tupled)
  @compileTimeOnly(aggMsg)
  def approxCountDistinct[T](a: T, b: Double): Long = ???

  @sparkAggregate(sf.avg(_:Column))
  @compileTimeOnly(aggMsg)
  def avg[T : Numeric](t: T): Double = ???

  @sparkAggregate(sf.collect_list(_:Column))
  @compileTimeOnly(aggMsg)
  def collect_list[T](t: T): Seq[T] = ???

  @sparkAggregate(sf.collect_set(_:Column))
  @compileTimeOnly(aggMsg)
  def collect_set[T](t: T): Seq[T] = ???

  @sparkAggregate((sf.corr(_:Column,_:Column)).tupled)
  @compileTimeOnly(aggMsg)
  def corr(a: Double, b: Double): Double = ???

  @sparkAggregate(sf.count(_:Column))
  @compileTimeOnly(aggMsg)
  def count[T](t: T): Long = ???

  @sparkAggregate(((col: Column, cols: Seq[Column]) => sf.countDistinct(col, cols:_*)).tupled)
  @compileTimeOnly(aggMsg)
  def countDistinct(columns: Any*): Long = ???

}
