package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers

class BitwiseTests extends TypedDatasetSuite with Matchers{

  /**
    * providing instances with implementations for bitwise operations since in the tests
    * we need to check the results from frameless vs the results from normal scala operators
    * for Numeric it is easy to test since scala comes with Numeric typeclass but there seems
    * to be no equivalent typeclass for bitwise ops for Byte Short Int and Long types supported in Catalyst
    */
  trait CatalystBitwise4Tests[A]{
    def bitwiseAnd(a1: A, a2: A): A
    def bitwiseOr(a1: A, a2: A): A
    def bitwiseXor(a1: A, a2: A): A
    def &(a1: A, a2: A): A = bitwiseAnd(a1, a2)
    def |(a1: A, a2: A): A = bitwiseOr(a1, a2)
    def ^(a1: A, a2: A): A = bitwiseXor(a1, a2)
  }

  object CatalystBitwise4Tests {
    implicit val framelessbyteBitwise      : CatalystBitwise4Tests[Byte]       = new CatalystBitwise4Tests[Byte] {
      def bitwiseOr(a1: Byte, a2: Byte) : Byte = (a1 | a2).toByte
      def bitwiseAnd(a1: Byte, a2: Byte): Byte = (a1 & a2).toByte
      def bitwiseXor(a1: Byte, a2: Byte): Byte = (a1 ^ a2).toByte
    }
    implicit val framelessshortBitwise     : CatalystBitwise4Tests[Short]      = new CatalystBitwise4Tests[Short] {
      def bitwiseOr(a1: Short, a2: Short) : Short = (a1 | a2).toShort
      def bitwiseAnd(a1: Short, a2: Short): Short = (a1 & a2).toShort
      def bitwiseXor(a1: Short, a2: Short): Short = (a1 ^ a2).toShort
    }
    implicit val framelessintBitwise       : CatalystBitwise4Tests[Int]        = new CatalystBitwise4Tests[Int] {
      def bitwiseOr(a1: Int, a2: Int) : Int = a1 | a2
      def bitwiseAnd(a1: Int, a2: Int): Int = a1 & a2
      def bitwiseXor(a1: Int, a2: Int): Int = a1 ^ a2
    }
    implicit val framelesslongBitwise      : CatalystBitwise4Tests[Long]       = new CatalystBitwise4Tests[Long] {
      def bitwiseOr(a1: Long, a2: Long) : Long = a1 | a2
      def bitwiseAnd(a1: Long, a2: Long): Long = a1 & a2
      def bitwiseXor(a1: Long, a2: Long): Long = a1 ^ a2
    }

  }
  import CatalystBitwise4Tests._
  test("bitwiseAND") {
    def prop[A: TypedEncoder: CatalystBitwise](a: A, b: A)(
      implicit catalystBitwise4Tests: CatalystBitwise4Tests[A]
    ): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[CatalystBitwise4Tests[A]].bitwiseAnd(a, b)
      val resultSymbolic = implicitly[CatalystBitwise4Tests[A]].&(a, b)
      val got = df.select(df.col(Symbol("a")) bitwiseAND df.col(Symbol("b"))).collect().run()
      val gotSymbolic = df.select(df.col(Symbol("a")) & b).collect().run()
      val symbolicCol2Col = df.select(df.col(Symbol("a")) & df.col(Symbol("b"))).collect().run()
      val canCast = df.select(df.col(Symbol("a")).cast[Long] & 0L).collect().run()
      canCast should contain theSameElementsAs Seq.fill[Long](gotSymbolic.size)(0L)
      result ?= resultSymbolic
      symbolicCol2Col ?= (result :: Nil)
      got ?= (result :: Nil)
      gotSymbolic ?= (resultSymbolic :: Nil)
    }

    check(prop[Byte] _)
    check(prop[Short] _)
    check(prop[Int] _)
    check(prop[Long] _)
  }

  test("bitwiseOR") {
    def prop[A: TypedEncoder: CatalystBitwise](a: A, b: A)(
      implicit catalystBitwise4Tests: CatalystBitwise4Tests[A]
    ): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[CatalystBitwise4Tests[A]].bitwiseOr(a, b)
      val resultSymbolic = implicitly[CatalystBitwise4Tests[A]].|(a, b)
      val got = df.select(df.col(Symbol("a")) bitwiseOR df.col(Symbol("b"))).collect().run()
      val gotSymbolic = df.select(df.col(Symbol("a")) | b).collect().run()
      val symbolicCol2Col = df.select(df.col(Symbol("a")) | df.col(Symbol("b"))).collect().run()
      val canCast = df.select(df.col(Symbol("a")).cast[Long] | -1L).collect().run()
      canCast should contain theSameElementsAs Seq.fill[Long](gotSymbolic.size)(-1L)
      result ?= resultSymbolic
      symbolicCol2Col ?= (result :: Nil)
      got ?= (result :: Nil)
      gotSymbolic ?= (resultSymbolic :: Nil)
    }

    check(prop[Byte] _)
    check(prop[Short] _)
    check(prop[Int] _)
    check(prop[Long] _)
  }

  test("bitwiseXOR") {
    def prop[A: TypedEncoder: CatalystBitwise](a: A, b: A)(
      implicit catalystBitwise4Tests: CatalystBitwise4Tests[A]
    ): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[CatalystBitwise4Tests[A]].bitwiseXor(a, b)
      val resultSymbolic = implicitly[CatalystBitwise4Tests[A]].^(a, b)
      result ?= resultSymbolic
      val got = df.select(df.col(Symbol("a")) bitwiseXOR df.col(Symbol("b"))).collect().run()
      val gotSymbolic = df.select(df.col(Symbol("a")) ^ b).collect().run()
      val zeroes = df.select(df.col(Symbol("a")) ^ df.col(Symbol("a"))).collect().run()
      zeroes should contain theSameElementsAs Seq.fill[Long](gotSymbolic.size)(0L)
      got ?= (result :: Nil)
      gotSymbolic ?= (resultSymbolic :: Nil)
    }

    check(prop[Byte] _)
    check(prop[Short] _)
    check(prop[Int] _)
    check(prop[Long] _)
  }
}
