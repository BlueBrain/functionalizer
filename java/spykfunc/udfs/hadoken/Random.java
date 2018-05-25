package spykfunc.udfs.hadoken;

import org.apache.commons.math3.random.AbstractRandomGenerator;

public class Random extends AbstractRandomGenerator {
    static {
        System.loadLibrary("random");
    }

    public Random() {
        nativeHandle = createThreefry();
    }

    public Random(long handle) {
        nativeHandle = handle;
    }

    public static Random create(long seed, long key1, long key2) {
        return new Random(internalCreate(seed, key1, key2));
    }

    public Random derivate(long key) {
        return new Random(internalDerivate(key));
    }

    public native void dispose();
    public native double nextDouble();
    public native long nextLong();
    public native void setSeed(long seed);

    private native long createThreefry();
    private native static long internalCreate(long seed, long key1, long key2);
    private native long internalDerivate(long key);

    private long nativeHandle;
}
