package spykfunc.udfs;

import org.apache.spark.sql.api.java.UDF1;  //UDF0 will make it soon to release
import random.Ziggurat;
import java.util.Random;


public class GaussRand implements UDF1<Integer, Float> {
    private Random rng = null;

    @Override
    public Float call(Integer seed) throws Exception {
        // UDF0 doesnt exist, so we profit to receive the seed 
        if (rng == null) {
            if (seed == 0) {
                rng = new Random();
            }
            else {
                rng = new Random(seed);
            }
        }
        return new Float(Ziggurat.nextGaussian(rng));
    }
}

