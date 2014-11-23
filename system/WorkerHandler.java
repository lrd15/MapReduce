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
    }

    @Override
    public void run() {
    	System.out.println("Worker Handler #" + id + " running...");
        try {
        	// Set timeout in ms
			socket.setSoTimeout(Configuration.TIMEOUT);
			toWorker = new ObjectOutputStream(socket.getOutputStream());
			fromWorker = new ObjectInputStream(socket.getInputStream());
			while (running) {
                Object obj = fromWorker.readObject();
                if (obj instanceof Signal) {
                    Signal sig = (Signal)obj;
                    MapJob mapJob = null;
                    MapJobSplit split = null;
                    switch (sig.getSignal()) {
                        case HEARTBEAT:
//                        	System.out.println("Received heartbeat.");
                            alive = true;
                            break;
                        case MAP_COMPLETED:
                        	System.out.println("Received MAP_COMPLETED signal (Job = " + jobID + ", SplitID = " + idx + ").");
                            String[] filenames = (String[])fromWorker.readObject();
//                            System.out.println("Received " + filenames.length + " intermediate files.");
                            mapJob = master.getMapJob(jobID);
                            split = mapJob.getSplit(idx);
                            split.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
//                            if (isIdle())
//                            	System.out.println("Worker (" + getSocket().getInetAddress() + ") now idle.");
//                            else
//                            	System.out.println("Worker (" + getSocket().getInetAddress() + ") failed to become idle.");
                            split.setIntermediateFilenames(filenames);
//                            System.out.println("Filename0: " + split.getIntermediateFilename(0));
//                            System.out.println("First intermediate filename for MapJob #" + jobID + ": " + master.getMapJob(jobID).getSplit(idx).getIntermediateFilename(0));
                            mapJob.incNumCompleted();
                            if (mapJob.isCompleted())
                                master.migrate(mapJob);
                            break;
                        case REDUCE_COMPLETED:
                        	System.out.println("Received REDUCE_COMPLETED signal (Job = " + jobID + ", PartitionID = " + idx + ").");
                            ReduceJob reduceJob = master.getReduceJob(jobID);
                            ReducePartition partition = reduceJob.getPartition(idx);
                            partition.setJobState(JobState.COMPLETED);
                            setWorkerState(WorkerState.IDLE);
                            reduceJob.incNumCompleted();
                            if (reduceJob.isCompleted()) {
                            	master.removeMapJob(jobID);
                                master.removeReduceJob(reduceJob);
                                master.removeJob(reduceJob.getID());
                            }
                            break;
                        case MAP_FAILED:
                        	System.out.println("Received MAP_FAILED signal (Job = " + jobID + ", SplitID = " + idx + ").");
                        	mapJob = master.getMapJob(jobID);
                            split = mapJob.getSplit(idx);
                            split.setJobState(JobState.IDLE);
                            setWorkerState(WorkerState.IDLE);
                            break;
                        case REDUCE_FAILED:
                        	System.out.println("Received REDUCE_FAILED signal (Job = " + jobID + ", PartitionID = " + idx + ").");
                        	master.fallback(jobID);
                        	setWorkerState(WorkerState.IDLE);
                        	break;
                        default:
                        	System.out.println("Unexpected signal received: " + sig.getSignal());
                        	break;
                    }
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
			alive = false;
			running = false;
			master.killWorkerHandler(this);
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