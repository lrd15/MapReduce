package system;

public class Signal {
    public static final int MAP_COMPLETED = 1;
    public static final int REDUCE_COMPLETED = 2;
    public static final int HEARTBEAT = 3;
    public static final int INIT_MAP = 4;
    public static final int INIT_REDUCE = 5;
    public static final int ADD_JOB = 6;

    private int signal;

    public Signal(int signal) {
        this.signal = signal;
    }

    public int getSignal() {
        return signal;
    }
}