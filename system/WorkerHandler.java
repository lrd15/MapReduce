package system;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;

import config.Configuration;

public class WorkerHandler extends Thread {
    // Status codes
    public static final int MAP_JOB = 1;
    public static final int REDUCE_JOB = 2;
    public static final int NONE = 0;

    private int id;
    private boolean alive;
    private Socket socket;
    private ObjectInputStream fromWorker;
    private ObjectOutputStream toWorker;

    private JobTracker master;

    private WorkerState state;
    private boolean running;

    private int jobID, idx, status;

    public WorkerHandler(JobTracker master, int id, Socket socket) throws IOException {
        this.master = master;
        this.id = id;
        this.socket = socket;
        alive = true;
        state = WorkerState.IDLE;
        jobID = idx = -1;
        status = NONE;
        running = true;
        this.socket = socket;
        fromWorker = new ObjectInputStream(socket.getInputStream());
        toWorker = new ObjectOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        socket.setSoTimeout(conf.TIMEOUT); // Set timeout in ms
        while (running) {
            try {
                Object obj = fromWorker.readObject();
                if (obj instanceof Signal) {
                    Signal sig = (Signal)obj;
                    switch (sig.getSignal()) {
                        case HEARTBEAT:
                            alive = true;
                            break;
                        case MAP_COMPLETED:
                            // Code to get filenames

                            String[] filenames = null; // TODO
                            MapJob job = master.getMapJob(jobID);
                            MapJobSplit split = job.getSplit(idx);
                            split.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
                            split.setIntermediateFilenames(filenames);
                            job.incNumCompleted();
                            if (job.isCompleted())
                                master.migrate(job);
                            break;
                        case REDUCE_COMPLETED:
                            ReduceJob job = master.getReduceJob(jobID);
                            ReducePartition partition = job.getPartition(idx);
                            partition.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
                            job.incNumCompleted();
                            if (job.isCompleted())
                                master.removeReduceJob(job);
                            break;
                    }
                }
            } catch (SocketTimeoutException e) { // Timeout -> tracker dies
                alive = false;
                running = false;
            }
        }
        master.removeWorkerHandler(this);
    }

    synchronized public Object readObject() {
        return fromWorker.readObject();
    }

    synchronized public void writeObject(Object obj) {
        toWorker.writeObject(obj);
    }

    public void setJobStatus(int status, int jobID, int idx) {
        this.status = status;
        this.jobID = jobID;
        this.idx = idx;
    }

    public int getJobStatus() {
        return status;
    }

    public int getJobID() {
        return jobID;
    }

    public int getIdx() {
        return idx;
    }

    public Socket getSocket() {
        return socket;
    }

    public WorkerState getWorkerState() {
        return state;
    }

    public void setWorkerState(WorkerState s) {
        state = s;
    }

    public boolean isIdle() {
        return state == WorkerState.IDLE;
    }

    public boolean alive() {
        return alive;
    }

    public int getID() {
        return id;
    }
}