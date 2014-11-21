package system;

import java.io.Serializable;
import java.net.InetAddress;

public class Host implements Serializable{
	
    private InetAddress ipAddress;
    private int portForClient;
    private int portForWorker;

    public Host(InetAddress ipAddress, int portForClient, int portForWorker) {
        this.ipAddress = ipAddress;
        this.portForClient = portForClient;
        this.portForWorker = portForWorker;
    }

    public InetAddress getIPAddress() {
        return ipAddress;
    }

    public int getPortForClient() {
        return portForClient;
    }

    public int getPortForWorker() {
        return portForWorker;
    }
}