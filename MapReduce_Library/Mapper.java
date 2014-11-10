public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;

    public Mapper(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
        this.context = context;
    }

    abstract void map(KEYIN key, VALUEIN value);

    private void emit(KEYOUT key, VALUEOUT value) {
        context.write(key, value);
    }

    public void run() {
        if (context.getCurrentKey() != null) { // Initally has key-value pair
            do {
                KEYIN key;
                VALUEIN value;
                key = context.getCurrentKey();
                value = context.getCurrentValue();
                map(key, value);
            } while (context.nextKeyValue()); // Advance to next key-value pair
        }
    }

    public void setContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
        this.context = context;
    }

    public MapContext getContext() {
        return MapContext context;
    }
}