import sbt._

// Copied, with some modifications, from https://github.com/milessabin/shapeless/blob/master/project/Boilerplate.scala

object Boilerplate {
  import scala.StringContext._

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated split '\n'
      val trimmedLines = rawLines map { _ dropWhile (_.isWhitespace) }
      trimmedLines mkString "\n"
    }
  }

  val header = "// Auto-generated boilerplate"

  val templates: Seq[Template] = List(
    GenXLTuples,
    GenXLTuplerInstances
  )

  def gen(dir: File) =
    for(template <- templates) yield {
      val tgtFile = template.filename(dir)
      IO.write(tgtFile, template.body)
      tgtFile
    }

  class TemplateVals(val arity: Int) {
    val synTypes     = (0 until arity) map (n => s"A$n")
    val synVals      = (0 until arity) map (n => s"a$n")
    val synTypedVals = (synVals zip synTypes) map { case (v,t) => v + ": " + t}
    val `A..N`       = synTypes.mkString(", ")
    val `A::N`       = (synTypes :+ "HNil") mkString "::"
    val `a::n`       = (synVals :+ "HNil") mkString "::"
    val `a..n`       = synVals.mkString(", ")
    val `_.._`       = Seq.fill(arity)("_").mkString(", ")
    val `(A..N)`     = synTypes.mkString(s"Tuple$arity[", ", ", "]")
    val `(a..n)`     = synVals.mkString(s"Tuple$arity(", ", ", ")")
    val `a:A..n:N`   = synTypedVals.mkString(", ")
  }

  abstract class Template(minArity: Int, maxArity: Int) {
    def filename(root: File): File
    def content(tv: TemplateVals): String
    def range = minArity to maxArity
    def body: String = {
      val headerLines = header split '\n'
      val rawContents = range map { n => content(new TemplateVals(n)) split '\n' filterNot (_.isEmpty) }
      val preBody = rawContents.head takeWhile (_ startsWith "|") map (_.tail)
      val instances = rawContents flatMap {_ filter (_ startsWith "-") map (_.tail) }
      val postBody = rawContents.head dropWhile (_ startsWith "|") dropWhile (_ startsWith "-") map (_.tail)
      (headerLines ++ preBody ++ instances ++ postBody) mkString "\n"
    }
  }

  object GenXLTuples extends Template(23, 64) {
    def filename(root: File) = root / "XLTuples.scala"
    
    def content(tv: TemplateVals) = {
      import tv._
      val typed_a = (1 to arity).map(n => s"_$n: ").zip(synTypes).map(p => p._1 + p._2)
      val `case a-1 => _a..case n-1 => _n` = (1 to arity).map(n => s"case ${n-1} => _$n").mkString("; ")
      val `def _a: A..def _n: N` = typed_a.map("def ".+).mkString("; ")
      val `_a: A.._n: N` = typed_a.mkString(", ")
      val `+A..+N` = synTypes.map("+".+).mkString(", ")
      block"""
        |package scala
        -
        -object Product${arity} {
        -  def unapply[${`A..N`}](x: Product${arity}[${`A..N`}]): Option[Product${arity}[${`A..N`}]] = Some(x)
        -}
        -
        -trait Product${arity}[${`+A..+N`}] extends Any with Product {
        -  override def productArity = ${arity}
        -  override def productElement(n: Int) = n match {
        -    ${`case a-1 => _a..case n-1 => _n`}
        -    case _ => throw new IndexOutOfBoundsException(n.toString())
        -  }
        -  ${`def _a: A..def _n: N`}
        -}
        -
        -final case class Tuple${arity}[${`+A..+N`}](${`_a: A.._n: N`}) extends Product${arity}[${`A..N`}]
        |
      """
    }
  }
  
  object GenXLTuplerInstances extends Template(1, 64) {
    def filename(root: File) = root / "XLTuplerInstances.scala"
    def content(tv: TemplateVals) = {
      import tv._
      block"""
        |package typedframe
        |
        |import shapeless._
        |
        |trait XLTuplerInstances {
        |  type Aux[L <: HList, Out0] = XLTupler[L] { type Out = Out0 }
        -
        -  implicit def hlistTupler${arity}[${`A..N`}]: Aux[${`A::N`}, ${`(A..N)`}] =
        -    new XLTupler[${`A::N`}] {
        -      type Out = ${`(A..N)`}
        -      def apply(l : ${`A::N`}): Out = l match { case ${`a::n`} => ${`(a..n)`} }
        -    }        
        |}
      """
    }      
  }

}
