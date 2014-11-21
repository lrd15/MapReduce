package system;

import java.io.Serializable;

public class Signal implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private SigNum signal;

    public Signal(SigNum signal) {
        this.signal = signal;
    }

    public SigNum getSignal() {
        return signal;
    }
    
}