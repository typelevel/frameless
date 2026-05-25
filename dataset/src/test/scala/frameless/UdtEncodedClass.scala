package frameless

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeArrayData
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.FramelessInternals.UserDefinedType

@SQLUserDefinedType(udt = classOf[UdtEncodedClassUdt])
class UdtEncodedClass(val a: Int, val b: Array[Double]) {

  override def equals(other: Any): Boolean = other match {
    case that: UdtEncodedClass =>
      a == that.a && java.util.Arrays.equals(b, that.b)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[Any](a, b)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"UdtEncodedClass($a, $b)"
}

object UdtEncodedClass {
  implicit val udtForUdtEncodedClass = new UdtEncodedClassUdt
}

class UdtEncodedClassUdt extends UserDefinedType[UdtEncodedClass] {

  def sqlType: DataType = {
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField(
          "b",
          ArrayType(DoubleType, containsNull = false),
          nullable = false
        )
      )
    )
  }

  def serialize(obj: UdtEncodedClass): InternalRow = {
    val row = new GenericInternalRow(3)
    row.setInt(0, obj.a)
    row.update(1, UnsafeArrayData.fromPrimitiveArray(obj.b))
    row
  }

  def deserialize(datum: Any): UdtEncodedClass = datum match {
    case row: InternalRow =>
      new UdtEncodedClass(row.getInt(0), row.getArray(1).toDoubleArray())
  }

  def userClass: Class[UdtEncodedClass] = classOf[UdtEncodedClass]
}
