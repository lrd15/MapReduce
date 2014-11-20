package system;

public class Signal {

    private SignalNum signal;

    public Signal(SignalNum signal) {
        this.signal = signal;
    }

    public SignalNum getSignal() {
        return signal;
    }
    
}

enum SignalNum {
	MAP_COMPLETED,
	REDUCE_COMPLETED,
	HEARTBEAT,
	INIT_MAP,
	INIT_REDUCE,
	ADD_JOB
}