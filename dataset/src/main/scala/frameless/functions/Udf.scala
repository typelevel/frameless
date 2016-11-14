package frameless
package functions

import org.apache.spark.sql.catalyst.expressions.ScalaUDF

/** Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
trait Udf {

  /** Defines a user-defined function of 1 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A, R: TypedEncoder](f: A => R):
    TypedColumn[A] => TypedColumn[R] = { u =>
      val aenc = u.uencoder

      val scalaUdf = ScalaUDF(f, TypedEncoder[R].targetDataType, Seq(u.expr),
        Seq(aenc.targetDataType))
      new TypedColumn[R](scalaUdf)
  }

  /** Defines a user-defined function of 2 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, R: TypedEncoder](f: (A1, A2) => R):
    (TypedColumn[A1], TypedColumn[A2]) => TypedColumn[R] = { (u1, u2) =>
      val (a1enc, a2enc) = (u1.uencoder, u2.uencoder)

      val scalaUdf = ScalaUDF(f, TypedEncoder[R].targetDataType, Seq(u1.expr, u2.expr),
        Seq(a1enc.targetDataType, a2enc.targetDataType))
      new TypedColumn[R](scalaUdf)
    }

  /** Defines a user-defined function of 3 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, R: TypedEncoder](f: (A1, A2, A3) => R):
  (TypedColumn[A1], TypedColumn[A2], TypedColumn[A3]) => TypedColumn[R] = { (u1, u2, u3) =>
      val (a1enc, a2enc, a3enc) = (u1.uencoder, u2.uencoder, u3.uencoder)

      val scalaUdf = ScalaUDF(f, TypedEncoder[R].targetDataType, Seq(u1.expr, u2.expr, u3.expr),
        Seq(a1enc.targetDataType, a2enc.targetDataType, a3enc.targetDataType))
      new TypedColumn[R](scalaUdf)
    }

  /** Defines a user-defined function of 4 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, A4, R: TypedEncoder](f: (A1, A2, A3, A4) => R):
    (TypedColumn[A1], TypedColumn[A2], TypedColumn[A3], TypedColumn[A4]) => TypedColumn[R] = { (u1, u2, u3, u4) =>
      val (a1enc, a2enc, a3enc, a4enc) = (u1.uencoder, u2.uencoder, u3.uencoder, u4.uencoder)

      val scalaUdf = ScalaUDF(f, TypedEncoder[R].targetDataType, Seq(u1.expr, u2.expr, u3.expr, u4.expr),
        Seq(a1enc.targetDataType, a2enc.targetDataType, a3enc.targetDataType, a4enc.targetDataType))
      new TypedColumn[R](scalaUdf)
    }

  /** Defines a user-defined function of 5 arguments as user-defined function (UDF).
    * The data types are automatically inferred based on the function's signature.
    *
    * apache/spark
    */
  def udf[T, A1, A2, A3, A4, A5, R: TypedEncoder](f: (A1, A2, A3, A4, A5) => R):
    (TypedColumn[A1], TypedColumn[A2], TypedColumn[A3], TypedColumn[A4],  TypedColumn[A5]) => TypedColumn[R] = { (u1, u2, u3, u4, u5) =>
      val (a1enc, a2enc, a3enc, a4enc, a5enc) = (u1.uencoder, u2.uencoder, u3.uencoder, u4.uencoder, u5.uencoder)

      val scalaUdf = ScalaUDF(f, TypedEncoder[R].targetDataType, Seq(u1.expr, u2.expr, u3.expr, u4.expr, u5.expr),
        Seq(a1enc.targetDataType, a2enc.targetDataType, a3enc.targetDataType, a4enc.targetDataType, a5enc.targetDataType))
      new TypedColumn[R](scalaUdf)
    }
}
