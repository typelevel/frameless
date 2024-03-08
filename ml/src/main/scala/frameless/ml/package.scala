package frameless

import FramelessInternals.UserDefinedType
import org.apache.spark.sql.shim.{ utils => MLFramelessInternals }
import org.apache.spark.ml.linalg.{ Matrix, Vector }

package object ml {

  implicit val mlVectorUdt: UserDefinedType[Vector] =
    MLFramelessInternals.vectorUdt

  implicit val mlMatrixUdt: UserDefinedType[Matrix] =
    MLFramelessInternals.matrixUdt

}
