import  org.apache.spark.sql.{DataFrame, SQLContext}
import shapeless.test._
import eu.timepit.refined.auto._

object Usage {
  val df: DataFrame = null
  implicit val s: SQLContext = null
  
  case class Foo(a: Int, b: String)
  
  val foo = TypedFrame[Foo](df)
  
  def testAs() = {
    case class Bar(i: Int, j: String)
    case class C(j: String, i: Int)
    case class D(i: Int, j: String, k: Int)
    
    foo.as[Bar]()
    illTyped("foo.as[C]()")
    illTyped("foo.as[D]()")
  }
  
  def testCartesianJoin() = {
    case class Bar(i: Double, j: String)
    val p = foo.cartesianJoin(TypedFrame[Bar](df)): TypedFrame[(Int, String, Double, String)]
  }
  
  def testOrderBy() = {
    foo.orderBy('a): TypedFrame[Foo]
    foo.orderBy('a, 'b): TypedFrame[Foo]
    foo.orderBy('b, 'a): TypedFrame[Foo]
    
    illTyped("foo.orderBy()")
    illTyped("foo.orderBy('c)")
    illTyped("foo.orderBy('a, 'c)")
  }
  
  def testSelect() = {
    foo.select('a): TypedFrame[Tuple1[Int]]
    foo.select('a, 'b): TypedFrame[(Int, String)]
    foo.select('b, 'a): TypedFrame[(String, Int)]
    
    illTyped("foo.select()")
    illTyped("foo.select('c)")
    illTyped("foo.select('a, 'c)")
  }
  
  def testFilter() = {
    foo.filter(_.a > 2): TypedFrame[Foo]
    foo.filter(_.b.startsWith("+")): TypedFrame[Foo]
  }
  
  def testLimit() = {
    foo.limit(0)
    foo.limit(1)
    foo.limit(2)
    foo.limit(100000000)
    illTyped("foo.limit(-1)")
  }
  
  def testUnionAll() = {
    case class ABar(a: Int, i: Double)
    case class BBar(a: Int, b: String, i: Double)
    case class CBar(u: Boolean, b: String, a: Int)
    case class DBar(u: Boolean)
  
    foo.unionAll(foo): TypedFrame[(Int, String)]
    foo.unionAll(TypedFrame[ABar](df)): TypedFrame[(Int, String, Double)]
    foo.unionAll(TypedFrame[BBar](df)): TypedFrame[(Int, String, Double)]
    foo.unionAll(TypedFrame[CBar](df)): TypedFrame[(Int, String, Boolean)]
    foo.unionAll(TypedFrame[DBar](df)): TypedFrame[(Int, String, Boolean)]
  }
  
  def testIntersect() = {
    case class ABar(a: Int, i: Double)
    case class BBar(a: Int, b: String, i: Double)
    case class CBar(u: Boolean, b: String, a: Int)
    case class DBar(u: Boolean)
    
    foo.intersect(foo): TypedFrame[(Int, String)]
    foo.intersect(TypedFrame[ABar](df)): TypedFrame[Tuple1[Int]]
    foo.intersect(TypedFrame[BBar](df)): TypedFrame[(Int, String)]
    foo.intersect(TypedFrame[CBar](df)): TypedFrame[(Int, String)]
    foo.intersect(TypedFrame[DBar](df)): TypedFrame[Unit]
  }
  
  def testExcept() = {
    case class ABar(a: Int, i: Double)
    case class BBar(a: Int, b: String, i: Double)
    case class CBar(u: Boolean, b: String, a: Int)
    case class DBar(u: Boolean)
    
    foo.except(foo): TypedFrame[Unit]
    foo.except(TypedFrame[ABar](df)): TypedFrame[Tuple1[String]]
    foo.except(TypedFrame[BBar](df)): TypedFrame[Unit]
    foo.except(TypedFrame[CBar](df)): TypedFrame[Unit]
    foo.except(TypedFrame[DBar](df)): TypedFrame[(Int, String)]
  }
  
  def testSample() = {
    // .0 needed, https://github.com/fthomas/refined/issues/96
    foo.sample(true, 0.0)
    foo.sample(false, 1.0)
    foo.sample(true, 0.5)
    illTyped("foo.sample(true, -0.1)")
    illTyped("foo.sample(true, 1.1)")
  }
  
  def testRandomSplit() = {
    foo.randomSplit(Array(0.1, 0.2, 0.7))
    illTyped("foo.randomSplit(Array(0.1, 0.2, -0.7))")
  }
  
  def testExplode() = {
    foo.explode { case Foo(a, b) => b.split(" ").map(_ -> a.toString) }
      : TypedFrame[(Int, String, String, String)]
    foo.explode(f => List.fill(f.a)(f.b -> f.b.isEmpty))
      : TypedFrame[(Int, String, String, Boolean)]
  }
  
  def testDrop() = {
    foo.drop('a): TypedFrame[Tuple1[String]]
    foo.drop('b): TypedFrame[Tuple1[Int]]
    foo.drop('a, 'b): TypedFrame[Unit] 
    
    illTyped("foo.drop()")
    illTyped("foo.drop('c)")
  }
  
  def testDropDuplicates() = {
    foo.dropDuplicates(): TypedFrame[Foo]
    foo.dropDuplicates('a): TypedFrame[Foo]
    foo.dropDuplicates('b): TypedFrame[Foo]
    foo.dropDuplicates('a, 'b): TypedFrame[Foo]
    illTyped("foo.dropDuplicates('c)")
  }
  
  def testDescribe() = {
    foo.describe('a): TypedFrame[Foo]
    foo.describe('b): TypedFrame[Foo]
    foo.describe('a, 'b): TypedFrame[Foo]
    illTyped("foo.describe()")
    illTyped("foo.describe('c)")
  }
  
  
  def testRepartition() = {
    foo.repartition(1): TypedFrame[Foo]  
    illTyped("foo.repartition(0)")
    illTyped("foo.repartition(-1)")
  }
  
  def testCoalesce() = {
    foo.coalesce(1): TypedFrame[Foo]  
    illTyped("foo.coalesce(0)")
    illTyped("foo.coalesce(-1)")
  }
  
  def testShow() = {
    foo.show(1)
    illTyped("foo.show(0)")
    illTyped("foo.show(-1)")
  }
  
  def testGroupBy() = {
    foo.groupBy('a)
    foo.groupBy('b)
    foo.groupBy('a, 'b)
    illTyped("foo.groupBy()")
    illTyped("foo.groupBy('c)")
  }
  
  def testRollup() = {
    foo.rollup('a)
    foo.rollup('b)
    foo.rollup('a, 'b)
    illTyped("foo.rollup()")
    illTyped("foo.rollup('c)")
  }
  
  def testCube() = {
    foo.cube('a)
    foo.cube('b)
    foo.cube('a, 'b)
    illTyped("foo.cube()")
    illTyped("foo.cube('c)")
  }
  
  def testHead() = {
    foo.head(): Foo
  }
  
  def testTake() = {
    foo.take(0): Seq[Foo]
    foo.take(1): Seq[Foo]
    illTyped("foo.take(-1)")
  }
  
  def testReduce() = {
    foo.reduce({ case (f1, f2) => Foo(f1.a + f2.a, f1.b) }): Foo
  }
  
  def testMap() = {
    foo.map(_.b -> 12): TypedFrame[(String, Int)]
  }
  
  def testFlatMap() = {
    foo.flatMap(f => List.fill(f.a)(f.b -> f.b.isEmpty)): TypedFrame[(String, Boolean)]
  }
  
  def testMapPartitions() = {
    foo.mapPartitions(_.map(_.b -> 12)): TypedFrame[(String, Int)]
  }
  
  def testForeach() = {
    foo.foreach(f => println(f.a))
  }
  
  def testForeachPartition() = {
    foo.foreachPartition(i => println(i.map(_.b).mkString(":")))
  }
  
  def testCollect() = {
    foo.collect(): Seq[Foo]
  }
  
  def testDropAny() = {
    foo.na.dropAny(): TypedFrame[Foo]
    foo.na.dropAny('a): TypedFrame[Foo]
    foo.na.dropAny('b): TypedFrame[Foo]
    foo.na.dropAny('a, 'b): TypedFrame[Foo]
    illTyped("foo.na.dropAny('c)")
  }
  
  def testDropAll() = {
    foo.na.dropAll(): TypedFrame[Foo]
    foo.na.dropAll('a): TypedFrame[Foo]
    foo.na.dropAll('b): TypedFrame[Foo]
    foo.na.dropAll('a, 'b): TypedFrame[Foo]
    illTyped("foo.na.dropAll('c)")
  }
  
  def testNaDrop() = {
    foo.na.drop(1)('a)
    foo.na.drop(1)('a, 'b)
    illTyped("foo.na.drop(0)('a)")
    illTyped("foo.na.drop(1)()")
    illTyped("foo.na.drop(1)('c)")
  }
  
  def testFill() = {
    case class Ts(i: Int, l: Long, f: Float, d: Double, s: String, b: Boolean, c: Char)
    val ts = TypedFrame[Ts](df)
    
    ts.na.fill(1)('i)
    ts.na.fill(1l)('l)
    ts.na.fill(1f)('f)
    ts.na.fill(1d)('d)
    ts.na.fill("string")('s)
    ts.na.fill(false)('b)
    
    illTyped("ts.na.fill(false)('s)")
    illTyped("ts.na.fill('c')('c)")
    
    case class Fs(i: Int, j: Int, s: String, k: Int, t: String)
    val fs = TypedFrame[Fs](df)
    
    fs.na.fill(1)('i)
    fs.na.fill(1)('i, 'j)
    fs.na.fill(1)('k, 'i)
    fs.na.fill("string")('s, 't)
    
    illTyped("ts.na.fill(1)('i, 's)")
    illTyped("ts.na.fill(1)()")
  }
  
  def testReplaceAll() = {
    case class Ts(d: Double, s: String, b: Boolean, c: Char)
    val ts = TypedFrame[Ts](df)
    
    ts.na.replaceAll(Map(1d -> 2d, 3d -> 4d))
    ts.na.replaceAll(Map("s" -> "S", "c" -> "C", "a" -> "A"))
    ts.na.replaceAll(Map(true -> false))
    
    illTyped("foo.na.replaceAll(Map('c' -> 'd'))")
    illTyped("foo.na.replaceAll(Map(true -> false))")
  }
  
  def testReplace() = {
    case class Ts(d: Double, s: String, b: Boolean, c: Char)
    val ts = TypedFrame[Ts](df)
  
    ts.na.replace(Map(1d -> 2d, 3d -> 4d))('d)
    ts.na.replace(Map("s" -> "S", "c" -> "C", "a" -> "A"))('s)
    ts.na.replace(Map(true -> false))('b)
    illTyped("foo.na.replace(Map('c' -> 'd'))('c)")
    
    case class Fs(i: Double, j: Double, s: String, k: Double, t: String)
    val fs = TypedFrame[Fs](df)
    
    fs.na.replace(Map(0d -> 1d))('i)
    fs.na.replace(Map(0d -> 1d))('i, 'j)
    fs.na.replace(Map(0d -> 1d))('k, 'i)
    fs.na.replace(Map("s" -> "S", "c" -> "C"))('s, 't)
    illTyped("ts.na.replace(1)('i, 's)")
    illTyped("ts.na.replace(Map(0 -> 1))()")
  }
  
  def testCov() = {
    case class Ns(i: Int, j: Int, d: Double, s: String)
    val ns = TypedFrame[Ns](df)
    ns.stat.cov('i, 'j)
    ns.stat.cov('i, 'd)
    illTyped("ns.stat.cov('i, 's)")
  }
  
  def testCorr() = {
    case class Ns(i: Int, j: Int, d: Double, s: String)
    val ns = TypedFrame[Ns](df)
    ns.stat.corr('i, 'j)
    ns.stat.corr('i, 'd)
    illTyped("ns.stat.corr('i, 's)")
  }
  
  def testCrosstab() = {
    case class Ns(i: Int, j: Int, d: Double, s: String)
    val ns = TypedFrame[Ns](df)
    ns.stat.crosstab('i, 'j)
    ns.stat.crosstab('i, 'd)
    ns.stat.crosstab('i, 's)
  }
  
  def testFreqItems() = {
    foo.stat.freqItems(0.0)('a)
    foo.stat.freqItems(1.0)('b)
    foo.stat.freqItems(0.5)('a, 'b)
    illTyped("foo.stat.freqItems(0.5)()")
    illTyped("foo.stat.freqItems(-0.1)('a)")
    illTyped("foo.stat.freqItems(1.1)('a)")
  }
  
  def testSampleBy() = {
    foo.stat.sampleBy('a, Map(1 -> 0.5, -1 -> 1.0), 10l)
    foo.stat.sampleBy('b, Map("s" -> 0.0, "c" -> 0.5, "S" -> 0.1), 10l)
    illTyped("foo.stat.sampleBy('b, Map(1 -> 0.5), 10l)")
    illTyped("foo.stat.sampleBy('a, Map(1 -> 1.1), 10l)")
  }
}
