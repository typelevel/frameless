package frameless

import org.apache.spark.ml.FramelessInternals
import org.apache.spark.ml.linalg.{Matrix, Vector}

import org.apache.spark.sql.FramelessInternals.UserDefinedType

package object ml {

  implicit val mlVectorUdt: UserDefinedType[Vector] = FramelessInternals.vectorUdt

  implicit val mlMatrixUdt: UserDefinedType[Matrix] = FramelessInternals.matrixUdt

}
