package spykfunc.udfs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import org.apache.spark.sql.api.java.UDF1;

public class FloatArrayDeserializerLittleEndian implements UDF1<byte[], Float[]> {

    @Override
    public Float[] call(byte[] bytes) throws Exception {
        byte[] reordered = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i += 4) {
            reordered[i] = bytes[i + 3];
            reordered[i + 1] = bytes[i + 2];
            reordered[i + 2] = bytes[i + 1];
            reordered[i + 3] = bytes[i];
        }
        ByteBuffer b = ByteBuffer.wrap(reordered);
        List<Float> res = new ArrayList<Float>();
        for (int i = 0; i < b.array().length / 4; i++) {
            res.add(new Float(b.getFloat()));
        }
        return res.toArray(new Float[res.size()]);
    }
}
