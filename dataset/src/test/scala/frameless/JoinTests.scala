// package frameless

// import org.scalacheck.Prop
// import org.scalacheck.Prop._

// class JoinTests extends TypedDatasetSuite {
//   import scala.math.Ordering.Implicits._

//   test("ab.joinLeft(ac, ab.a, ac.a)") {
//     def prop[A: TypedEncoder : Ordering, B: Ordering, C: Ordering](left: List[X2[A, B]], right: List[X2[A, C]])(
//       implicit
//       lefte: TypedEncoder[X2[A, B]],
//       righte: TypedEncoder[X2[A, C]],
//       joinede: TypedEncoder[(X2[A, B], Option[X2[A, C]])]
//     ): Prop = {
//       val leftDs = TypedDataset.create(left)
//       val rightDs = TypedDataset.create(right)
//       val joinedDs = leftDs
//         .joinLeft(rightDs, leftDs.col('a), rightDs.col('a))
//         .collect().run().toVector.sorted

//       val rightKeys = right.map(_.a).toSet
//       val joined = {
//         for {
//           ab <- left
//           ac <- right if ac.a == ab.a
//         } yield (ab, Some(ac))
//       }.toVector ++ {
//         for {
//           ab <- left if !rightKeys.contains(ab.a)
//         } yield (ab, None)
//       }.toVector

//       (joined.sorted ?= joinedDs) && (joinedDs.map(_._1).toSet ?= left.toSet)
//     }

//     check(forAll(prop[Int, Long, String] _))
//   }

//   test("ab.join(ac, ab.a, ac.a)") {
//     def prop[A: TypedEncoder : Ordering, B: Ordering, C: Ordering](left: List[X2[A, B]], right: List[X2[A, C]])(
//       implicit
//       lefte: TypedEncoder[X2[A, B]],
//       righte: TypedEncoder[X2[A, C]],
//       joinede: TypedEncoder[(X2[A, B], X2[A, C])]
//     ): Prop = {
//       val leftDs = TypedDataset.create(left)
//       val rightDs = TypedDataset.create(right)
//       val joinedDs = leftDs
//         .join(rightDs, leftDs.col('a), rightDs.col('a))
//         .collect().run().toVector.sorted

//       val joined = {
//         for {
//           ab <- left
//           ac <- right if ac.a == ab.a
//         } yield (ab, ac)
//       }.toVector

//       joined.sorted ?= joinedDs
//     }

//     check(forAll(prop[Int, Long, String] _))
//   }
// }
