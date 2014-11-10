public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context;

    public Reducer(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
        this.context = context;
    }

    abstract void reduce(KEYIN key, Iterable<VALUEIN> values);

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
                reduce(key, value);
            } while (context.nextKeyValue()); // Advance to next key-value pair
        }
    }

    public void setContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
        this.context = context;
    }

    public ReduceContext getContext() {
        return ReduceContext context;
    }
}