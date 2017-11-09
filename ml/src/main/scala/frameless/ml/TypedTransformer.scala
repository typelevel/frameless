package frameless
package ml

import frameless.ops.SmartProject
import org.apache.spark.ml.Transformer
import shapeless.{Generic, HList}
import shapeless.ops.hlist.{Prepend, Tupler}

sealed trait TypedTransformer

/**
  * An AppendTransformer `transform` method takes as input a TypedDataset containing `Inputs` and
  * return a TypedDataset with `Outputs` columns appended to the input TypedDataset.
  */
abstract class AppendTransformer[Inputs, Outputs, InnerTransformer <: Transformer] private[ml] extends TypedTransformer {
  val transformer: InnerTransformer

  def transform[T, TVals <: HList, OutputsVals <: HList, OutVals <: HList, Out](ds: TypedDataset[T])(
    implicit smartProject: SmartProject[T, Inputs],
    tGen: Generic.Aux[T, TVals],
    outputsGen: Generic.Aux[Outputs, OutputsVals],
    prepend: Prepend.Aux[TVals, OutputsVals, OutVals],
    tupler: Tupler.Aux[OutVals, Out],
    outEncoder: TypedEncoder[Out]
  ): TypedDataset[Out] = {
    val transformed = transformer.transform(ds.dataset).as[Out](TypedExpressionEncoder[Out])
    TypedDataset.create[Out](transformed)
  }

}

object AppendTransformer {
  // Random name to a temp column added by a TypedTransformer (the proper name will be given by the Tuple-based encoder)
  private[ml] val tempColumnName = "I1X3T9CU1OP0128JYIO76TYZZA3AXHQ18RMI"
  private[ml] val tempColumnName2 = "I1X3T9CU1OP0128JYIO76TYZZA3AXHQ18RMJ"
  private[ml] val tempColumnName3 = "I1X3T9CU1OP0128JYIO76TYZZA3AXHQ18RMK"
}