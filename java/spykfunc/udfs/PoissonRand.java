package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.commons.math3.distribution.PoissonDistribution;

public class PoissonRand extends Rand<PoissonDistribution>implements UDF2<Short, Float, Integer> {
    @Override
    public Integer call(Short id, Float mean) throws Exception {
        return this.get(id, () -> new PoissonDistribution(mean)).sample();
    }
}
