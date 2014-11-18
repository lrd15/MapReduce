import java.net.Socket;
import java.net.InetAddress;

public class TaskTracker {
    private Socket socket;
    private DataInputStream fromHandler;
    private DataOutputStream toHandler;

    private Configuration conf;

    private ServerSocket clientServerSocket, workerServerSocket;

    public TaskTracker(Configuration conf) {
        this.conf = conf;
        socket = new Socket(conf.getMaster().getAddress(), conf.getMaster().getWorkerPort());
        fromHandler = new DataInputStream(socket.getInputStream());
        toHandler = new DataOutputStream(socket.getOutputStream());
    }
}