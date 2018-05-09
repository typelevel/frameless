package frameless


class ExplodeTests extends TypedDatasetSuite {
  test("simple explode test") {
    val ds = TypedDataset.create(Seq((1,Array(1,2))))
    ds.explode('_2): TypedDataset[(Int,Int)]
  }
}
