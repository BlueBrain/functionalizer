package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF1;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import scala.collection.mutable.WrappedArray;
import scala.collection.Iterator;

public class IntArraySerializer implements UDF1<WrappedArray<Integer>, byte[]> {

    @Override
    public byte[] call(WrappedArray<Integer> nrs) throws Exception {
        ByteBuffer b = ByteBuffer.allocate(nrs.length()*4);
        b.order(ByteOrder.nativeOrder());
        Iterator<Integer> it = nrs.toIterator();
        while(it.hasNext()) {
            b.putInt(it.next());
        }
        return b.array();
    }
}
