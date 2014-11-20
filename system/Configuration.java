import java.util.ArrayList;
public class Configuration {
    // public final int CLIENT_PORT = 15640;
    // public final int WORKER_PORT = 6789;
    public final int TIMEOUT = 30000;
    public final int NUM_OF_REDUCERS = 2;
    public final int NUM_OF_MAPPERS = 10;
    
    private Host master;
    private ArrayList<Host> workerList;

    public Host getMaster() {
        return master;
    }

    public Host getWorkerByAddress(String ipAddress) {
        for (Host h : workerList)
            if (h.getAddress().getHostAddress().equals(ipAddress))
                return h;
        return null;
    }

    public ArrayList<Host> getWorkers() {
        return workerList;
    }
}