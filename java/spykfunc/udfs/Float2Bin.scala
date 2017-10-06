package spykfunc.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes.BinaryType
import scala.Array
import java.nio.ByteBuffer


class Float2Bin() extends Serializable {
    
    def float2bin(input_array: Seq[Float]) : Array[Byte] = {
        var b : ByteBuffer = ByteBuffer.allocate(input_array.length*4)
        var n = .0
        for( n <- input_array ) {
            b.putFloat(x)
        }
        return b.array()
    }
    
    def getUDF(): UserDefinedFunction = udf(float2bin _)
}
