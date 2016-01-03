package typedframe

import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped
import shapeless.{::, HNil}

class TestXLTuples extends SpecWithContext {
  import testImplicits._
  
  type Tuple23Ints = Tuple23[
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int]
  
  type HList23Ints =
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::HNil
    
  type Tuple46Ints = Tuple46[
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int]
  
  type HList46Ints =
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::Int::
    Int::Int::Int::Int::Int::Int::Int::HNil
  
  test("23x23 cartesianJoin") {
    val tSeq = Seq(Tuple23(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23))
    
    val tdf: TypedFrame[Tuple23Ints] = new TypedFrame(tSeq.toDF)
    val largeTypedFrame: TypedFrame[Tuple46Ints] = tdf.cartesianJoin(tdf)
    
    val joinedSeq: Seq[Tuple46Ints] = Seq(Tuple46(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23))
    
    checkAnswer(largeTypedFrame, joinedSeq)
  }
  
  test("IsXLTuple") {
    val is23Tuple: IsXLTuple[Tuple23Ints] = implicitly
    val is46Tuple: IsXLTuple[Tuple46Ints] = implicitly
  }
  
  test("XLTupler") {
    val tupler23: HList23Ints => Tuple23Ints = XLTupler[HList23Ints].apply
    val tupler46: HList46Ints => Tuple46Ints = XLTupler[HList46Ints].apply
  }
}
