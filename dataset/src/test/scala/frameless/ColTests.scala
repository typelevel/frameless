package frameless

import shapeless.test.illTyped

class ColTests extends TypedDatasetSuite {
  test("col") {
    val dataset = TypedDataset.create[X4[Int, String, Long, Boolean]](Nil)

    dataset.col('a)
    dataset.col[Int]('a)
    illTyped("dataset.col[String]('a)", "No column .* of type String in frameless.X4.*")

    dataset.col('b)
    dataset.col[String]('b)
    illTyped("dataset.col[Int]('b)", "No column .* of type Int in frameless.X4.*")

    ()
  }
}
