import java.io.*;
import java.net.Socket;
public class TaskTrackerClientHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    private Configuration conf;

    public TaskTrackerClientHandler(Configuration conf, int id, Socket socket) {
        this.id = id;
        this.socket = socket;
        this.conf = conf;

        this.socket = socket;
        fromClient = new ObjectInputStream(socket.getInputStream());
        toClient = new ObjectOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        while (running) {
            Object obj = fromClient.readObject();
            if (obj instanceof Signal) {
                Signal sig = (Signal)obj;
                switch (sig.getSignal()) {
                
                }
            }
        }
    }
}