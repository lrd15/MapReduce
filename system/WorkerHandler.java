import java.io.*;
import java.net.Socket;
public class WorkerHandler extends Thread {
    private int id;
    private boolean alive;
    private Socket socket;
    private DataInputStream fromWorker;
    private DataOutputStream toWorker;

    private Configuration conf;

    private State state;

    public WorkerHandler(Configuration conf, int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        alive = true;
        state = State.IDLE;
        this.socket = socket;
        fromWorker = new DataInputStream(socket.getInputStream());
        toWorker = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        socket.setSoTimeout(conf.TIMEOUT); // Set timeout in ms
        while (true) {
            try {
                int signal = fromWorker.readInt();
                switch (signal) {
                    case Signal.HEARTBEAT:
                        alive = true;
                        break;
                    case Signal.MAP_COMPLETED:
                        // Code here
                        break;
                    case Signal.REDUCE_COMPLETED:
                        // Code here
                        break;
                }
            } catch (SocketTimeoutException e) { // Timeout -> tracker dies
                alive = false;
            }
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public void setStateToBusy() {
        state = State.BUSY;
    }

    public void setStateToIdle() {
        state = State.IDLE;
    }

    public boolean isIdle() {
        return state == State.IDLE;
    }

    public boolean isAlive() {
        return alive;
    }

    private enum State {
        IDLE, BUSY
    }
}