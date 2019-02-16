package frameless

import scala.reflect.{ClassTag, classTag}
import shapeless._

object InjectionEnum {
  type InjectionEnum[A] = Injection[A, String]

  def apply[A: InjectionEnum]: InjectionEnum[A] = implicitly[InjectionEnum[A]]

  def instance[A](f: A => String, g: String => A): InjectionEnum[A] =
    Injection(f, g)

  implicit val cnilInjectionEnum: InjectionEnum[CNil] =
    instance(
      _ => throw new Exception("Impossible"),
      name =>
        throw new IllegalArgumentException(
          s"Cannot construct a value of type CNil: $name did not match data constructor names"
        )
    )

  implicit def coproductInjectionEnum[H: ClassTag, T <: Coproduct](
    implicit
    gen: Generic.Aux[H, HNil],
    tInjectionEnum: InjectionEnum[T]
    ): InjectionEnum[H :+: T] = {
    val canonicalName = classTag[H].runtimeClass.getCanonicalName

    instance(
      {
        case Inl(_) => canonicalName
        case Inr(t) => tInjectionEnum.apply(t)
      },
      name =>
        if (name == canonicalName)
          Inl(gen.from(HNil))
        else
          Inr(tInjectionEnum.invert(name))
    )
  }

  implicit def genericInjectionEnum[A, R](
    implicit
    gen: Generic.Aux[A, R],
    rInjectionEnum: InjectionEnum[R]
    ): InjectionEnum[A] =
    instance(
      value =>
        rInjectionEnum.apply(gen.to(value)),
      name =>
        gen.from(rInjectionEnum.invert(name))
    )
}
