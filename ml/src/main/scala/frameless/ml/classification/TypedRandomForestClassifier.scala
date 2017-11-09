package frameless
package ml
package classification

import frameless.ml.classification.TypedRandomForestClassifier.FeatureSubsetStrategy
import frameless.ml.internals.SelectorByValue
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.linalg.Vector
import shapeless.ops.hlist.Length
import shapeless.{HList, LabelledGeneric, Nat, Witness}
import scala.annotation.implicitNotFound

final class TypedRandomForestClassifier[Inputs] private[ml](
  rf: RandomForestClassifier,
  labelCol: String,
  featuresCol: String
) extends TypedEstimator[Inputs, TypedRandomForestClassifier.Outputs, RandomForestClassificationModel] {

  val estimator: RandomForestClassifier =
    rf
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)
      .setRawPredictionCol(AppendTransformer.tempColumnName2)
      .setProbabilityCol(AppendTransformer.tempColumnName3)

  def setNumTrees(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setNumTrees(value))
  def setMaxDepth(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMaxDepth(value))
  def setMinInfoGain(value: Double): TypedRandomForestClassifier[Inputs] = copy(rf.setMinInfoGain(value))
  def setMinInstancesPerNode(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMinInstancesPerNode(value))
  def setMaxMemoryInMB(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMaxMemoryInMB(value))
  def setSubsamplingRate(value: Double): TypedRandomForestClassifier[Inputs] = copy(rf.setSubsamplingRate(value))
  def setFeatureSubsetStrategy(value: FeatureSubsetStrategy): TypedRandomForestClassifier[Inputs] =
    copy(rf.setFeatureSubsetStrategy(value.sparkValue))
  def setMaxBins(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMaxBins(value))

  private def copy(newRf: RandomForestClassifier): TypedRandomForestClassifier[Inputs] =
    new TypedRandomForestClassifier[Inputs](newRf, labelCol, featuresCol)
}

object TypedRandomForestClassifier {
  case class Outputs(rawPrediction: Vector, probability: Vector, prediction: Double)

  sealed trait FeatureSubsetStrategy {
    val sparkValue: String
  }
  object FeatureSubsetStrategy {
    case object Auto extends FeatureSubsetStrategy {
      val sparkValue = "auto"
    }
    case object All extends FeatureSubsetStrategy {
      val sparkValue = "all"
    }
    case object OneThird extends FeatureSubsetStrategy {
      val sparkValue = "onethird"
    }
    case object Sqrt extends FeatureSubsetStrategy {
      val sparkValue = "sqrt"
    }
    case object Log2 extends FeatureSubsetStrategy {
      val sparkValue = "log2"
    }
    case class StrictlyPositiveDouble(value: Double) extends FeatureSubsetStrategy {
      val sparkValue = value.toString
    }
  }

  def create[Inputs]()
                    (implicit inputsChecker: TypedRandomForestClassifierInputsChecker[Inputs])
  : TypedRandomForestClassifier[Inputs] = {
    new TypedRandomForestClassifier(new RandomForestClassifier(), inputsChecker.labelCol, inputsChecker.featuresCol)
  }
}

@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type for TypedRandomForestClassifier. " +
    "Input type must only contain a field of type Double (label) and a field of type Vector (features)."
)
private[ml] trait TypedRandomForestClassifierInputsChecker[Inputs] {
  val labelCol: String
  val featuresCol: String
}

private[ml] object TypedRandomForestClassifierInputsChecker {
  implicit def checkInputs[
  Inputs,
  InputsRec <: HList,
  LabelK <: Symbol,
  FeaturesK <: Symbol](
    implicit
    inputsGen: LabelledGeneric.Aux[Inputs, InputsRec],
    sizeCheck: Length.Aux[InputsRec, Nat._2],
    labelSelect: SelectorByValue.Aux[InputsRec, Double, LabelK],
    labelW: Witness.Aux[LabelK],
    featuresSelect: SelectorByValue.Aux[InputsRec, Vector, FeaturesK],
    featuresW: Witness.Aux[FeaturesK]
  ): TypedRandomForestClassifierInputsChecker[Inputs] = {
    new TypedRandomForestClassifierInputsChecker[Inputs] {
      val labelCol: String = labelW.value.name
      val featuresCol: String = featuresW.value.name
    }
  }
}