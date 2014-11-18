import java.io.*;
import java.net.Socket;
public class WorkerHandler extends Thread {
    public static final int MAP_JOB = 1;
    public static final int REDUCE_JOB = 2;
    public static final int NONE = 0;
    private int id;
    private boolean alive;
    private Socket socket;
    private ObjectInputStream fromWorker;
    private ObjectOutputStream toWorker;

    private Configuration conf;

    private WorkerState state;

    private int jobId, idx, status;

    public WorkerHandler(Configuration conf, int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        alive = true;
        state = WorkerState.IDLE;
        jobId = idx = -1;
        status = NONE;

        this.socket = socket;
        fromWorker = new ObjectInputStream(socket.getInputStream());
        toWorker = new ObjectOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        socket.setSoTimeout(conf.TIMEOUT); // Set timeout in ms
        while (true) {
            try {
                Object obj = fromWorker.readObject();
                if (obj instanceof Signal) {
                    Signal sig = (Signal)obj;
                    switch (sig.getSignal()) {
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
                }
            } catch (SocketTimeoutException e) { // Timeout -> tracker dies
                alive = false;
            }
        }
    }

    synchronized public Object readObject() {
        return fromWorker.readObject();
    }

    synchronized public void writeObject(Object obj) {
        toWorker.writeObject(obj);
    }

    public void setJobStatus(int status, int jobId, int idx) {
        this.status = status;
        this.jobId = jobId;
        this.idx = idx;
    }

    public int getJobStatus() {
        return status;
    }

    public int getJobId() {
        return jobId;
    }

    public int getIdx() {
        return idx;
    }

    public Socket getSocket() {
        return socket;
    }

    public WorkerState getState() {
        return state;
    }

    public void setState(WorkerState s) {
        state = s;
    }

    public boolean isIdle() {
        return state == WorkerState.IDLE;
    }

    public boolean isAlive() {
        return alive;
    }

    public int getId() {
        return id;
    }
}