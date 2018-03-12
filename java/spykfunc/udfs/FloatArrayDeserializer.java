package spykfunc.udfs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import org.apache.spark.sql.api.java.UDF1;

public class FloatArrayDeserializer implements UDF1<byte[], Float[]> {

    @Override
    public Float[] call(byte[] bytes) throws Exception {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        List<Float> res = new ArrayList<Float>();
        for (int i = 0; i < b.array().length / 4; i++) {
            res.add(new Float(b.getFloat()));
        }
        return res.toArray(new Float[res.size()]);
    }
}
