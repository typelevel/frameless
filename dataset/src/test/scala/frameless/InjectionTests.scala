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

sealed trait Food
case object Burger extends Food
case object Pasta extends Food
case object Rice extends Food

object Food {
  implicit val arbitrary: Arbitrary[Food] =
    Arbitrary(Arbitrary.arbitrary[Int].map(i => injection.invert(Math.abs(i % 3))))

  implicit val injection: Injection[Food, Int] =
    Injection(
      {
        case Burger => 0
        case Pasta => 1
        case Rice => 2
      },
      {
        case 0 => Burger
        case 1 => Pasta
        case 2 => Rice
      }
    )
}

// Supposingly coming from a java lib, shapeless can't derive stuff for this one :(
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

  implicit val injection: Injection[Person, (Int, String)] =
    Injection(p => unapply(p).get, tupled)
}

case class I[A](value: A)

object I {
  implicit def injection[A]: Injection[I[A], A] = Injection(_.value, I(_))
  implicit def typedEncoder[A: TypedEncoder]: TypedEncoder[I[A]] = TypedEncoder.usingInjection[I[A], A]
  implicit def arbitrary[A: Arbitrary]: Arbitrary[I[A]] = Arbitrary(Arbitrary.arbitrary[A].map(I(_)))
}

sealed trait Employee
case object Casual extends Employee
case object PartTime extends Employee
case object FullTime extends Employee

object Employee {
  implicit val arbitrary: Arbitrary[Employee] =
    Arbitrary(Gen.oneOf(Casual, PartTime, FullTime))
}

sealed trait Maybe
case object Nothing extends Maybe
case class Just(get: Int) extends Maybe

sealed trait Switch
object Switch {
  case object Off extends Switch
  case object On extends Switch

  implicit val arbitrary: Arbitrary[Switch] =
    Arbitrary(Gen.oneOf(Off, On))
}

sealed trait Pixel
case class Red() extends Pixel
case class Green() extends Pixel
case class Blue() extends Pixel

object Pixel {
  implicit val arbitrary: Arbitrary[Pixel] =
    Arbitrary(Gen.oneOf(Red(), Green(), Blue()))
}

sealed trait Connection[+A]
case object Closed extends Connection[Nothing]
case object Open extends Connection[Nothing]

object Connection {
  implicit def arbitrary[A]: Arbitrary[Connection[A]] =
    Arbitrary(Gen.oneOf(Closed, Open))
}

sealed abstract class Vehicle(colour: String)
case object Car extends Vehicle("red")
case object Bike extends Vehicle("blue")

object Vehicle {
  implicit val arbitrary: Arbitrary[Vehicle] =
    Arbitrary(Gen.oneOf(Car, Bike))
}

class InjectionTests extends TypedDatasetSuite {
  test("Injection based encoders") {
    check(forAll(prop[Country] _))
    check(forAll(prop[LocalDateTime] _))
    check(forAll(prop[Food] _))
    check(forAll(prop[X1[Country]] _))
    check(forAll(prop[X1[LocalDateTime]] _))
    check(forAll(prop[X1[Food]] _))
    check(forAll(prop[X1[X1[Country]]] _))
    check(forAll(prop[X1[X1[LocalDateTime]]] _))
    check(forAll(prop[X1[X1[Food]]] _))
    check(forAll(prop[X2[Country, X2[LocalDateTime, Food]]] _))
    check(forAll(prop[X3[Country, LocalDateTime, Food]] _))
    check(forAll(prop[X3U[Country, LocalDateTime, Food]] _))

    check(forAll(prop[I[Int]] _))
    check(forAll(prop[I[Option[Int]]] _))
    check(forAll(prop[I[I[Int]]] _))
    check(forAll(prop[I[I[Option[Int]]]] _))

    check(forAll(prop[I[X1[Int]]] _))
    check(forAll(prop[I[I[X1[Int]]]] _))
    check(forAll(prop[I[I[Option[X1[Int]]]]] _))

    check(forAll(prop[Option[I[Int]]] _))
    check(forAll(prop[Option[I[X1[Int]]]] _))

    assert(TypedEncoder[I[Int]].catalystRepr == TypedEncoder[Int].catalystRepr)
    assert(TypedEncoder[I[I[Int]]].catalystRepr == TypedEncoder[Int].catalystRepr)

    assert(TypedEncoder[I[Option[Int]]].nullable)
  }

  test("TypedEncoder[Person] is ambiguous") {
    illTyped("implicitly[TypedEncoder[Person]]", "ambiguous implicit values.*")
  }

  test("Resolve ambiguity by importing usingInjection") {
    import TypedEncoder.usingInjection

    check(forAll(prop[X1[Person]] _))
    check(forAll(prop[X1[X1[Person]]] _))
    check(forAll(prop[X2[Person, Person]] _))
    check(forAll(prop[Person] _))

    assert(TypedEncoder[Person].catalystRepr == TypedEncoder[(Int, String)].catalystRepr)
  }

  test("Resolve ambiguity by importing usingDerivation") {
    import TypedEncoder.usingDerivation
    assert(implicitly[TypedEncoder[Person]].isInstanceOf[RecordEncoder[Person, _, _]])
    check(forAll(prop[Person] _))
  }

  test("TypedEncoder[Employee] implicit is missing") {
    illTyped(
      "implicitly[TypedEncoder[Employee]]",
      "could not find implicit value for parameter e.*"
    )
  }

  test("Resolve missing implicit by deriving Injection instance") {
    import frameless.TypedEncoder.injections._

    check(forAll(prop[X1[Employee]] _))
    check(forAll(prop[X1[X1[Employee]]] _))
    check(forAll(prop[X2[Employee, Employee]] _))
    check(forAll(prop[Employee] _))

    assert(TypedEncoder[Employee].catalystRepr == TypedEncoder[String].catalystRepr)
  }

  test("TypedEncoder[Maybe] cannot be derived") {
    import frameless.TypedEncoder.injections._

    illTyped(
      "implicitly[TypedEncoder[Maybe]]",
      "could not find implicit value for parameter e.*"
    )
  }

  test("Derive encoder for type with data constructors defined in the companion object") {
    import frameless.TypedEncoder.injections._

    check(forAll(prop[X1[Switch]] _))
    check(forAll(prop[X1[X1[Switch]]] _))
    check(forAll(prop[X2[Switch, Switch]] _))
    check(forAll(prop[Switch] _))

    assert(TypedEncoder[Switch].catalystRepr == TypedEncoder[String].catalystRepr)
  }

  test("Derive encoder for type with data constructors defined as parameterless case classes") {
    import frameless.TypedEncoder.injections._

    check(forAll(prop[X1[Pixel]] _))
    check(forAll(prop[X1[X1[Pixel]]] _))
    check(forAll(prop[X2[Pixel, Pixel]] _))
    check(forAll(prop[Pixel] _))

    assert(TypedEncoder[Pixel].catalystRepr == TypedEncoder[String].catalystRepr)
  }

  test("Derive encoder for phantom type") {
    import frameless.TypedEncoder.injections._

    check(forAll(prop[X1[Connection[Int]]] _))
    check(forAll(prop[X1[X1[Connection[Int]]]] _))
    check(forAll(prop[X2[Connection[Int], Connection[Int]]] _))
    check(forAll(prop[Connection[Int]] _))

    assert(TypedEncoder[Connection[Int]].catalystRepr == TypedEncoder[String].catalystRepr)
  }

  test("Derive encoder for ADT with abstract class as the base type") {
    import frameless.TypedEncoder.injections._

    check(forAll(prop[X1[Vehicle]] _))
    check(forAll(prop[X1[X1[Vehicle]]] _))
    check(forAll(prop[X2[Vehicle, Vehicle]] _))
    check(forAll(prop[Vehicle] _))

    assert(TypedEncoder[Vehicle].catalystRepr == TypedEncoder[String].catalystRepr)
  }

  test("apply method of derived Injection instance produces the correct string") {
    import frameless.TypedEncoder.injections._

    assert(implicitly[Injection[Employee, String]].apply(Casual) === "Casual")
    assert(implicitly[Injection[Switch, String]].apply(Switch.On) === "On")
    assert(implicitly[Injection[Pixel, String]].apply(Blue()) === "Blue")
    assert(implicitly[Injection[Connection[Int], String]].apply(Open) === "Open")
    assert(implicitly[Injection[Vehicle, String]].apply(Bike) === "Bike")
  }

  test("invert method of derived Injection instance produces the correct value") {
    import frameless.TypedEncoder.injections._

    assert(implicitly[Injection[Employee, String]].invert("Casual") === Casual)
    assert(implicitly[Injection[Switch, String]].invert("On") === Switch.On)
    assert(implicitly[Injection[Pixel, String]].invert("Blue") === Blue())
    assert(implicitly[Injection[Connection[Int], String]].invert("Open") === Open)
    assert(implicitly[Injection[Vehicle, String]].invert("Bike") === Bike)
  }

  test(
    "invert method of derived Injection instance should throw exception if string does not match data constructor names"
  ) {
    import frameless.TypedEncoder.injections._

    val caught = intercept[IllegalArgumentException] {
      implicitly[Injection[Employee, String]].invert("cassual")
    }

    assert(
      caught.getMessage ===
        "Cannot construct a value of type CNil: cassual did not match data constructor names"
    )
  }
}
