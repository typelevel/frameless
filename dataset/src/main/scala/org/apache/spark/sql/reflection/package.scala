package org.apache.spark.sql

import org.apache.spark.sql.catalyst.ScalaReflection.{cleanUpReflectionObjects, getClassFromType, localTypeOf}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ObjectType, ShortType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.catalyst.ScalaReflection.{isNativeType => oisNativeType}

/**
 * Copy of spark's pre 3.4 reflection based encoding
 */
package object reflection {

  /**
   * forwards to keep the rest of the code looking the same
   */
  def isNativeType(dt: DataType): Boolean = oisNativeType(dt)

  private object ScalaSubtypeLock

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe

  import universe._

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Special handling is also used for Arrays including
   * those that hold primitive types.
   *
   * Unlike `schemaFor`, this function doesn't do any massaging of types into the Spark SQL type
   * system.  As a result, ObjectType will be returned for things like boxed Integers
   */
  def dataTypeFor[T : TypeTag]: DataType = dataTypeFor(localTypeOf[T])

  /**
   * Synchronize to prevent concurrent usage of `<:<` operator.
   * This operator is not thread safe in any current version of scala; i.e.
   * (2.11.12, 2.12.10, 2.13.0-M5).
   *
   * See https://github.com/scala/bug/issues/10766
   */
  private[sql] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  private def dataTypeFor(tpe: `Type`): DataType = cleanUpReflectionObjects {
    tpe.dealias match {
      case t if isSubtype(t, definitions.NullTpe) => NullType
      case t if isSubtype(t, definitions.IntTpe) => IntegerType
      case t if isSubtype(t, definitions.LongTpe) => LongType
      case t if isSubtype(t, definitions.DoubleTpe) => DoubleType
      case t if isSubtype(t, definitions.FloatTpe) => FloatType
      case t if isSubtype(t, definitions.ShortTpe) => ShortType
      case t if isSubtype(t, definitions.ByteTpe) => ByteType
      case t if isSubtype(t, definitions.BooleanTpe) => BooleanType
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => BinaryType
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => CalendarIntervalType
      case t if isSubtype(t, localTypeOf[Decimal]) => DecimalType.SYSTEM_DEFAULT
      case _ =>
        /* original Spark code checked for scala.Array vs ObjectType,
           this (and associated code) isn't needed due to TypedEncoders arrayEncoder */
        val clazz = getClassFromType(tpe)
        ObjectType(clazz)
    }
  }

}
