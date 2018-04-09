package spykfunc.udfs;

import java.util.HashMap;
import java.util.function.Supplier;

public abstract class Rand<T> {
    private HashMap<Integer, T> rngs;

    public Rand() {
        rngs = new HashMap<Integer, T>();
    }

    public T get(Integer id, Supplier<T> gen) {
        Class<T> cls;
        if (! rngs.containsKey(id))
            rngs.put(id, gen.get());
        return rngs.get(id);
    }
}
