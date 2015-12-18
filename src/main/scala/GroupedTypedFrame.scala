package typedframe

import shapeless._
import org.apache.spark.sql.GroupedData
import shapeless.ops.hlist.{ToList, Prepend}
import shapeless.ops.record.{SelectAll, Values, Keys}

final class GroupedTypedFrame[Schema <: HList, Vals <: HList](gd: GroupedData) extends Serializable {
  // def agg(exprs: Column*): DataFrame =
  
  object avg extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList,  V <: HList, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        v: Values.Aux[Schema, V],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        a: LiftAll[Numeric, S],
        p: Prepend.Aux[V, S, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.avg(l(columnTuple).map(_.name): _*))
  }
  
  object mean extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList,  V <: HList, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        v: Values.Aux[Schema, V],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        a: LiftAll[Numeric, S],
        p: Prepend.Aux[V, S, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.mean(l(columnTuple).map(_.name): _*))
  }
  
  object max extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList,  V <: HList, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        v: Values.Aux[Schema, V],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        a: LiftAll[Numeric, S],
        p: Prepend.Aux[V, S, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.max(l(columnTuple).map(_.name): _*))
  }
  
  object min extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList,  V <: HList, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        v: Values.Aux[Schema, V],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        a: LiftAll[Numeric, S],
        p: Prepend.Aux[V, S, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.min(l(columnTuple).map(_.name): _*))
  }
  
  object sum extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList,  V <: HList, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        v: Values.Aux[Schema, V],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        a: LiftAll[Numeric, S],
        p: Prepend.Aux[V, S, P],
        t: ManyTupler.Aux[P, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.sum(l(columnTuple).map(_.name): _*))
  }
  
  def count[Out <: Product, V <: HList, P <: HList, B <: HList, Y <: HList]()
    (implicit
      v: Values.Aux[Schema, V],
      p: Prepend.Aux[V, Int :: HNil, P],
      t: ManyTupler.Aux[P, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(gd.count)
}
