package system;

import java.io.*;
import java.net.Socket;
public class ClientHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    private Configuration conf;
    private JobTracker master;
    private boolean running;

    public ClientHandler(JobTracker master, Configuration conf, int id, Socket socket) throws IOException {
        this.master = master;
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        this.socket = socket;
        running = true;
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
                    case Signal.ADD_JOB:
                        toClient.writeObject(new Integer(id));
                        break;
                }
            }
        }
    }
}