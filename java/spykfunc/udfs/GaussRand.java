package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.commons.math3.distribution.NormalDistribution;


public class GaussRand implements UDF2<Float, Float, Float> {
    @Override
    public Float call(Float mean, Float sd) throws Exception {
        return (float) (new NormalDistribution(mean, sd).sample());
    }
}

