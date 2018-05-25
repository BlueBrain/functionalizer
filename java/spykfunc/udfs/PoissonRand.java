package spykfunc.udfs;

import spykfunc.udfs.hadoken.Random;

import org.apache.spark.sql.api.java.UDF4;
import org.apache.commons.math3.distribution.PoissonDistribution;

public class PoissonRand implements UDF4<Integer, Integer, Integer, Float, Integer> {
    @Override
    public Integer call(Integer pre, Integer post, Integer id, Float mean) throws Exception {
        Random r = Random.create(pre, post, id);
        Integer value = new PoissonDistribution(r,
                                                mean,
                                                PoissonDistribution.DEFAULT_EPSILON,
                                                PoissonDistribution.DEFAULT_MAX_ITERATIONS).sample();
        r.dispose();
        return value;
    }
}
