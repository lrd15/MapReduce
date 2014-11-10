public class Host {
    private InetAddress ipAddr;
    private int port;
    private int coreNum;

    public Host(InetAddress ipAddr, int port, int coreNum) {
        this.ipAddr = ipAddr;
        this.port = port;
        this.coreNum = coreNum;
    }

    public Host(Host h) {
        this.ipAddr = h.getIpAddress();
        this.port = h.getPort();
        this.coreNum = h.getCoreNumber();
    }

    public InetAddress getIpAddress() {
        return ipAddr;
    }

    public int getPort() {
        return port;
    }

    public int getCoreNumber() {
        return coreNum;
    }
}