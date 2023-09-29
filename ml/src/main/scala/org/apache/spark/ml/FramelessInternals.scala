package org.apache.spark.ml

import org.apache.spark.ml.linalg.{MatrixUDT, VectorUDT}

object FramelessInternals {

  // because org.apache.spark.ml.linalg.VectorUDT is private[spark]
  val vectorUdt = new VectorUDT

  // because org.apache.spark.ml.linalg.MatrixUDT is private[spark]
  val matrixUdt = new MatrixUDT

}
