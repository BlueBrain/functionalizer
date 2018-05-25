package spykfunc.udfs;

import spykfunc.udfs.hadoken.Random;

import org.apache.spark.sql.api.java.UDF5;
import org.apache.commons.math3.distribution.NormalDistribution;

public class GaussRand implements UDF5<Integer, Integer, Integer, Float, Float, Double> {
    @Override
    public Double call(Integer pre, Integer post, Integer id, Float mean, Float sd) throws Exception {
        Random r = Random.create(pre, post, id);
        Double value = new NormalDistribution(r, mean, sd).sample();
        r.dispose();
        return value;
    }
}

