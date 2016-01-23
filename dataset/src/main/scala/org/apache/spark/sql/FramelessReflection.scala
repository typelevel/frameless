package org.apache.spark.sql

import org.apache.spark.sql.types.ObjectType

import scala.reflect.ClassTag

object FramelessReflection {
  def objectTypeFor[A](implicit classTag: ClassTag[A]): ObjectType = ObjectType(classTag.runtimeClass)
}
