public class Host {
    private InetAddress address;
    private int clientPort, workerPort;

    public Host(InetAddress address, int clientPort, int workerPort) {
        this.address = address;
        this.clientPort = clientPort;
        this.workerPort = workerPort;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getWorkerPort() {
        return workerPort;
    }
}