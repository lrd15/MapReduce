import java.io.*;
import java.net.Socket;
public class ClientHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    private Configuration conf;
    private JobTracker master;

    public ClientHandler(JobTracker master, Configuration conf, int id, Socket socket) throws IOException {
        this.master = master;
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        this.socket = socket;
        fromClient = new ObjectInputStream(socket.getInputStream());
        toClient = new ObjectOutputStream(socket.getOutputStream());
    }
}