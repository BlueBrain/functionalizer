package spykfunc.udfs;

import spykfunc.udfs.hadoken.Random;

import org.apache.spark.sql.api.java.UDF5;
import org.apache.commons.math3.distribution.GammaDistribution;

public class GammaRand implements UDF5<Integer, Integer, Integer, Float, Float, Double> {
    @Override
    public Double call(Integer pre, Integer post, Integer id, Float mean, Float sd) throws Exception {
        Float shape = mean * mean / (sd * sd);
        Float scale = sd * sd / mean;
        Random r = Random.create(pre, post, id);
        Double value = new GammaDistribution(r, shape, scale).sample();
        r.dispose();
        return value;
    }
}
