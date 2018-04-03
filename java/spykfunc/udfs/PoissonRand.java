package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.commons.math3.distribution.PoissonDistribution;


public class PoissonRand implements UDF1<Float, Integer> {
    @Override
    public Integer call(Float mean) throws Exception {
        return new PoissonDistribution(mean).sample();
    }
}
