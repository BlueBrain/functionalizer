package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.commons.math3.distribution.NormalDistribution;

public class GaussRand extends Rand<NormalDistribution> implements UDF3<Short, Float, Float, Float> {
    @Override
    public Float call(Short id, Float mean, Float sd) throws Exception {
        return (float) (this.get(id, () -> new NormalDistribution(mean, sd)).sample());
    }
}

