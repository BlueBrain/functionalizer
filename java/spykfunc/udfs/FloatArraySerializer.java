package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF1;
import java.nio.ByteBuffer;
import scala.collection.mutable.WrappedArray;
import scala.collection.Iterator;

public class FloatArraySerializer implements UDF1<WrappedArray<Float>, byte[]> {
    
    @Override
    public byte[] call(WrappedArray<Float> nrs) throws Exception {
        ByteBuffer b = ByteBuffer.allocate(nrs.length()*4);
        Iterator<Float> it = nrs.toIterator();
        while(it.hasNext()) {
            b.putFloat(it.next());
        }
        return b.array();
    }
}
