package system;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
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
        try {
        	// Set timeout in ms
			socket.setSoTimeout(Configuration.TIMEOUT);
			while (running) {
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
                            MapJob mapJob = master.getMapJob(jobID);
                            MapJobSplit split = mapJob.getSplit(idx);
                            split.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
                            split.setIntermediateFilenames(filenames);
                            mapJob.incNumCompleted();
                            if (mapJob.isCompleted())
                                master.migrate(mapJob);
                            break;
                        case REDUCE_COMPLETED:
                            ReduceJob reduceJob = master.getReduceJob(jobID);
                            ReducePartition partition = reduceJob.getPartition(idx);
                            partition.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
                            reduceJob.incNumCompleted();
                            if (reduceJob.isCompleted())
                                master.removeReduceJob(reduceJob);
                            break;
                        default:
                        	break;
                    }
                }
            } 
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SocketTimeoutException e) { // Timeout -> tracker dies
            alive = false;
            running = false;
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        master.removeWorkerHandler(this);
    }

    synchronized public Object readObject() throws ClassNotFoundException, IOException {
        return fromWorker.readObject();
    }

    synchronized public void writeObject(Object obj) throws IOException {
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