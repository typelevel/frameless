package frameless

import org.scalacheck.Arbitrary

case class X1[A](a: A)

object X1 {
  implicit def arbitrary[A: Arbitrary]: Arbitrary[X1[A]] = {
    Arbitrary(implicitly[Arbitrary[A]].arbitrary.map(X1(_)))
  }
}

case class X2[A, B](a: A, b: B)

object X2 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary]: Arbitrary[X2[A, B]] = {
    Arbitrary(Arbitrary.arbTuple2[A, B].arbitrary.map((X2.apply[A, B] _).tupled))
  }
}

case class X3[A, B, C](a: A, b: B, c: C)

object X3 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary, C: Arbitrary]: Arbitrary[X3[A, B, C]] = {
    Arbitrary(Arbitrary.arbTuple3[A, B, C].arbitrary.map((X3.apply[A, B, C] _).tupled))
  }
}

case class X4[A, B, C, D](a: A, b: B, c: C, d: D)

object X4 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary, C: Arbitrary, D: Arbitrary]: Arbitrary[X4[A, B, C, D]] = {
    Arbitrary(Arbitrary.arbTuple4[A, B, C, D].arbitrary.map((X4.apply[A, B, C, D] _).tupled))
  }
}

