import java.io.*;
import java.net.Socket;
public class ClientHandler extends Thread {
    private int id;
    private Socket socket;
    private DataInputStream fromClient;
    private DataOutputStream toClient;

    private Configuration conf;

    public ClientHandler(Configuration conf, int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        this.socket = socket;
        fromClient = new DataInputStream(socket.getInputStream());
        toClient = new DataOutputStream(socket.getOutputStream());
    }
}