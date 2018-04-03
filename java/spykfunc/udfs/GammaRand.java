package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.commons.math3.distribution.GammaDistribution;


public class GammaRand implements UDF2<Float, Float, Float> {
    @Override
    public Float call(Float mean, Float sd) throws Exception {
        Float shape = mean * mean / (sd * sd);
        Float scale = sd * sd / mean;
        return (float) (new GammaDistribution(shape, scale).sample());
    }
}
