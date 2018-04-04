package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.commons.math3.distribution.GammaDistribution;

public class GammaRand extends Rand<GammaDistribution> implements UDF3<Short, Float, Float, Float> {
    @Override
    public Float call(Short id, Float mean, Float sd) throws Exception {
        Float shape = mean * mean / (sd * sd);
        Float scale = sd * sd / mean;
        return (float) (this.get(id, () -> new GammaDistribution(shape, scale)).sample());
    }
}
