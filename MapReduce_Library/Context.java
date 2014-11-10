public abstract class Context<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    // Todo:
    // input files
    // output files
    // write(KEYOUT, VALUEOUT)

    public abstract KEYIN getCurrentKey();
    public abstract VALUEIN getCurrentValue();

    /**
     * Advance to next key-value pair
     *
     * @return true if next key-value pair exists, false otherwise
     */
    public abstract boolean nextKeyValue();
}