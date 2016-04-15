package frameless

import frameless.CollectTests.prop
import org.scalacheck._
import org.scalacheck.Prop._
import shapeless.test.illTyped

sealed trait Country
case object France extends Country
case object Russia extends Country

object Country {
  implicit val arbitrary: Arbitrary[Country] =
    Arbitrary(Arbitrary.arbitrary[Boolean].map(injection.invert))

  implicit val injection: Injection[Country, Boolean] =
    Injection(France.==, if (_) France else Russia)
}

// Supposingly comming from a java lib, shapeless can't derive stuff for this one :(
class LocalDateTime {
  var instant: Long = _

  override def equals(o: Any): Boolean =
    o.isInstanceOf[LocalDateTime] && o.asInstanceOf[LocalDateTime].instant == instant
}

object LocalDateTime {
  implicit val arbitrary: Arbitrary[LocalDateTime] =
    Arbitrary(Arbitrary.arbitrary[Long].map(injection.invert))

  implicit val injection: Injection[LocalDateTime, Long] =
    Injection(
      _.instant,
      long => { val ldt = new LocalDateTime; ldt.instant = long; ldt }
    )
}

case class Person(age: Int, name: String)

object Person {
  val tupled = (Person.apply _).tupled

  implicit val arbitrary: Arbitrary[Person] =
    Arbitrary(Arbitrary.arbTuple2[Int, String].arbitrary.map(tupled))

  implicit val injection: Injection[Person, Tuple2[Int, String]] =
    Injection(p => unapply(p).get, tupled)
}

class InjectionTests extends TypedDatasetSuite {
  test("Injection based encoders") {
    check(forAll(prop[Country] _))
    check(forAll(prop[LocalDateTime] _))
  }

  test("TypedEncoder[Person] is ambiguous") {
    illTyped("implicitly[TypedEncoder[Person]]", "ambiguous implicit values.*")
  }

  test("Resolve ambiguity by importing usingInjection") {
    import TypedEncoder.usingInjection
    assert(implicitly[TypedEncoder[Person]].isInstanceOf[PrimitiveTypedEncoder[Person]])
    check(forAll(prop[Person] _))
  }

  test("Resolve ambiguity by importing usingDerivation") {
    import TypedEncoder.usingDerivation
    assert(implicitly[TypedEncoder[Person]].isInstanceOf[RecordEncoder[Person, _]])
    check(forAll(prop[Person] _))
  }
}
