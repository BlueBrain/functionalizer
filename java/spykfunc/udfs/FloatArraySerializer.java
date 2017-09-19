package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF1;  //UDF0 will make it soon to release
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import scala.collection.mutable.WrappedArray;
import scala.collection.Iterator;

public class FloatArraySerializer implements UDF1<WrappedArray<Float>, byte[]> {
    
    @Override
    public byte[] call(WrappedArray<Float> nrs) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Iterator<Float> it = nrs.toIterator();
        while(it.hasNext()) {
            dos.writeFloat(it.next());
        }
        dos.flush();
        return bos.toByteArray();
    }
}
