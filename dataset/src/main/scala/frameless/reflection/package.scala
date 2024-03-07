package frameless

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Copy of spark's pre 3.4 reflection based encoding
 */
package object reflection {

  /**
   * copy of pre 3.5.0 isNativeType, https://issues.apache.org/jira/browse/SPARK-44343 removed it
   */
  def isNativeType(dt: DataType): Boolean = dt match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType |
        LongType | FloatType | DoubleType | BinaryType | CalendarIntervalType =>
      true
    case _ => false
  }

  private object ScalaSubtypeLock

  val universe: scala.reflect.runtime.universe.type =
    scala.reflect.runtime.universe

  import universe._

  // Since we are creating a runtime mirror using the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  def mirror: universe.Mirror = {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  /**
   * Any codes calling `scala.reflect.api.Types.TypeApi.<:<` should be wrapped by this method to
   * clean up the Scala reflection garbage automatically. Otherwise, it will leak some objects to
   * `scala.reflect.runtime.JavaUniverse.undoLog`.
   *
   * @see https://github.com/scala/bug/issues/8302
   */
  def cleanUpReflectionObjects[T](func: => T): T = {
    universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.undo(func)
  }

  /**
   * Return the Scala Type for `T` in the current classloader mirror.
   *
   * Use this method instead of the convenience method `universe.typeOf`, which
   * assumes that all types can be found in the classloader that loaded scala-reflect classes.
   * That's not necessarily the case when running using Eclipse launchers or even
   * Sbt console or test (without `fork := true`).
   *
   * @see SPARK-5281
   */
  def localTypeOf[T: TypeTag]: `Type` = {
    val tag = implicitly[TypeTag[T]]
    tag.in(mirror).tpe.dealias
  }

  /*
   * Retrieves the runtime class corresponding to the provided type.
   */
  def getClassFromType(tpe: Type): Class[_] =
    mirror.runtimeClass(erasure(tpe).dealias.typeSymbol.asClass)

  private def erasure(tpe: Type): Type = {
    // For user-defined AnyVal classes, we should not erasure it. Otherwise, it will
    // resolve to underlying type which wrapped by this class, e.g erasure
    // `case class Foo(i: Int) extends AnyVal` will return type `Int` instead of `Foo`.
    // But, for other types, we do need to erasure it. For example, we need to erasure
    // `scala.Any` to `java.lang.Object` in order to load it from Java ClassLoader.
    // Please see SPARK-17368 & SPARK-31190 for more details.
    if (isSubtype(tpe, localTypeOf[AnyVal]) && !tpe.toString.startsWith("scala")) {
      tpe
    } else {
      tpe.erasure
    }
  }

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Special handling is also used for Arrays including
   * those that hold primitive types.
   *
   * Unlike `schemaFor`, this function doesn't do any massaging of types into the Spark SQL type
   * system.  As a result, ObjectType will be returned for things like boxed Integers
   */
  def dataTypeFor[T: TypeTag]: DataType = dataTypeFor(localTypeOf[T])

  /**
   * Synchronize to prevent concurrent usage of `<:<` operator.
   * This operator is not thread safe in any current version of scala; i.e.
   * (2.11.12, 2.12.10, 2.13.0-M5).
   *
   * See https://github.com/scala/bug/issues/10766
   */
  private def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  private def dataTypeFor(tpe: `Type`): DataType = cleanUpReflectionObjects {
    tpe.dealias match {
      case t if isSubtype(t, definitions.NullTpe)      => NullType
      case t if isSubtype(t, definitions.IntTpe)       => IntegerType
      case t if isSubtype(t, definitions.LongTpe)      => LongType
      case t if isSubtype(t, definitions.DoubleTpe)    => DoubleType
      case t if isSubtype(t, definitions.FloatTpe)     => FloatType
      case t if isSubtype(t, definitions.ShortTpe)     => ShortType
      case t if isSubtype(t, definitions.ByteTpe)      => ByteType
      case t if isSubtype(t, definitions.BooleanTpe)   => BooleanType
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => BinaryType
      case t if isSubtype(t, localTypeOf[CalendarInterval]) =>
        CalendarIntervalType
      case t if isSubtype(t, localTypeOf[Decimal]) => DecimalType.SYSTEM_DEFAULT
      case _                                       =>
        /* original Spark code checked for scala.Array vs ObjectType,
           this (and associated code) isn't needed due to TypedEncoders arrayEncoder */
        val clazz = getClassFromType(tpe)
        ObjectType(clazz)
    }
  }

}
