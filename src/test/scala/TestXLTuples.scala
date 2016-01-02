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
    
  test("XLTuples") {
    type Tuple46Ints = Tuple46[
      Int, Int, Int, Int, Int, Int, Int, Int,
      Int, Int, Int, Int, Int, Int, Int, Int,
      Int, Int, Int, Int, Int, Int, Int,
      Int, Int, Int, Int, Int, Int, Int, Int,
      Int, Int, Int, Int, Int, Int, Int, Int,
      Int, Int, Int, Int, Int, Int, Int]
    
    val tSeq = Seq(Tuple23(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23))
    val tdf: TypedFrame[Tuple23Ints] = TypedFrame(tSeq.toDF)
    val largeTypedFrame: TypedFrame[Tuple46Ints] = tdf.cartesianJoin(tdf)
    
    // TODO: Takes forever to compile :(
    // checkAnswer(
    //   largeTypedFrame,
    //   Seq(Tuple46(
    //     1, 2, 3, 4, 5, 6, 7, 8,
    //     9, 10, 11, 12, 13, 14, 15, 16,
    //     17, 18, 19, 20, 21, 22, 23,
    //     1, 2, 3, 4, 5, 6, 7, 8,
    //     9, 10, 11, 12, 13, 14, 15, 16,
    //     17, 18, 19, 20, 21, 22, 23))
    // )
  }
  
  test("IsXLTuple") {
    val is23Tuple = IsXLTuple[Tuple23Ints]
  }

  test("XLTupler") {
    type HList23Ints =
      Int :: Int :: Int :: Int :: Int :: Int :: Int :: Int ::
      Int :: Int :: Int :: Int :: Int :: Int :: Int :: Int ::
      Int :: Int :: Int :: Int :: Int :: Int :: Int :: HNil
    
    val tupler23: HList23Ints => Tuple23Ints = XLTupler[HList23Ints].apply
  }
}
