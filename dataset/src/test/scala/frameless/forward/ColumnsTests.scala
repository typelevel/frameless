package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop.forAll

class ColumnsTests extends TypedDatasetSuite {
  test("columns") {
    def prop(
        i: Int,
        s: String,
        b: Boolean,
        l: Long,
        d: Double,
        by: Byte
      ): Prop = {
      val x1 = X1(i) :: Nil
      val x2 = X2(i, s) :: Nil
      val x3 = X3(i, s, b) :: Nil
      val x4 = X4(i, s, b, l) :: Nil
      val x5 = X5(i, s, b, l, d) :: Nil
      val x6 = X6(i, s, b, l, d, by) :: Nil

      val datasets = Seq(
        TypedDataset.create(x1),
        TypedDataset.create(x2),
        TypedDataset.create(x3),
        TypedDataset.create(x4),
        TypedDataset.create(x5),
        TypedDataset.create(x6)
      )

      Prop.all(datasets.flatMap { dataset =>
        val columns = dataset.dataset.columns
        dataset.columns.map(col => Prop.propBoolean(columns contains col))
      }: _*)
    }

    check(forAll(prop _))
  }
}
