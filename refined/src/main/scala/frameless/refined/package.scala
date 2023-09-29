package frameless

import scala.reflect.ClassTag

import eu.timepit.refined.api.{ RefType, Validate }

package object refined extends RefinedFieldEncoders {
  implicit def refinedInjection[F[_, _], T, R](
    implicit
      refType: RefType[F],
      validate: Validate[T, R]
    ): Injection[F[T, R], T] = Injection(
    refType.unwrap,
    { value =>
      refType.refine[R](value) match {
        case Left(errMsg) =>
          throw new IllegalArgumentException(
            s"Value $value does not satisfy refinement predicate: $errMsg")

        case Right(res) => res
      }
    })

  implicit def refinedEncoder[F[_, _], T, R](
    implicit
      i0: RefType[F],
      i1: Validate[T, R],
      i2: TypedEncoder[T],
      i3: ClassTag[F[T, R]]
    ): TypedEncoder[F[T, R]] = TypedEncoder.usingInjection(
    i3, refinedInjection, i2)
}

