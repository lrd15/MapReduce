package system;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.ServerSocket;
import java.net.Socket;

import config.Configuration;
import config.Job;

public class JobTracker extends Thread {
    private ArrayList<WorkerHandler> workerHandlerList;
    private ArrayList<ClientHandler> clientHandlerList;
    private HashMap<Integer, Job> jobMap;

    private ArrayList<MapJob> mapJobList;
    private ArrayList<ReduceJob> reduceJobList;

    private int nextClientID;
    private int nextWorkerID;

    // Round Robin between map and reduce, and between jobs
    private int curMapJobIdx, curReduceJobIdx;
    private boolean shouldDoMap;

    private boolean running;

    private ServerSocket clientServerSocket, workerServerSocket;
    
    public static final String INPUT_DIR = "input";
    public static final String OUTPUT_DIR = "output";

    public JobTracker() throws IOException {
        nextClientID = 0;
        nextWorkerID = 0;
        running = true;
        curMapJobIdx = curReduceJobIdx = 0;
        shouldDoMap = true;

        workerHandlerList = new ArrayList<WorkerHandler>();
        clientHandlerList = new ArrayList<ClientHandler>();
        jobMap = new HashMap<Integer, Job>();
        mapJobList = new ArrayList<MapJob>();
        reduceJobList = new ArrayList<ReduceJob>();
        clientServerSocket = new ServerSocket(Configuration.MASTER.getPortForClient());
        workerServerSocket = new ServerSocket(Configuration.MASTER.getPortForWorker());

        ClientListener clientListener = new ClientListener();
        clientListener.start();
        WorkerListener workerListener = new WorkerListener();
        workerListener.start();
    }

    @Override
    public void run() {
        // Start a separate thread to check if each worker is alive
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
        System.out.println("Heartbeat thread started.");
        
        while (running) {
            while (hasJob()) {
                boolean done = false;
                if (shouldDoMap) {
                    if (hasMapJob()) {
                        MapJob job = getCurrentMapJob();
                        MapJobSplit[] splits = job.getSplits();
                        for (int i = 0; i < splits.length; i++) {
                            MapJobSplit split = splits[i];
                            if (split.getJobState() != JobState.IDLE)
                                continue;
							try {
								Host[] hosts = split.getInputSplit().getLocations();
								for (Host host : hosts) {
                                    WorkerHandler wh = getWorkerHandler(host);
                                    if (wh != null && wh.isIdle()) { // Found idle worker
                                    	System.out.println("Initiating map operation...");
                                        done = initMap(wh, split, job.getID(), i);
                                        if (done) {
                                        	System.out.println("Worker (" + wh.getSocket().getLocalAddress() +
                                        			") assigned for map operation: JobID = " + job.getID() + ", SplitId = " + i);
                                        	break;
                                        }
                                        else
                                        	System.out.println("Map operation initiation failed.");
                                    }
                                }
							} catch (IOException e) {
								e.printStackTrace();
							}
                            if (done)
                                break;
                        }
                    }
                }
                else {
                    if (hasReduceJob()) {
                        ReduceJob job = getCurrentReduceJob();
                        if (job.hasNextIdlePartition()) {
                            int idx = job.nextIdlePartitionIdx();
                            ReducePartition partition = job.getPartition(idx);
                            for (WorkerHandler wh : workerHandlerList)
                                if (wh.isIdle()) { // Found idle worker
                                	System.out.println("Initiating reduce operatino...");
                                    done = initReduce(wh, partition, job.getID(), idx);
                                    if (done) {
                                    	System.out.println("Worker (" + wh.getSocket().getLocalAddress() +
                                    			") assigned for reduce operation: JobID = " + job.getID() + ", PartitionId = " + idx);
                                    	job.decNumIdle();
                                    	break;
                                    }
                                    else
                                    	System.out.println("Reduce operation initiation failed.");
                                }
                        }
                    }
                }
                toNextJob();
                if (done)
                    break;
            }
        }
    }
    
    public void addJob(Job job) {
    	jobMap.put(job.getID(), job);
    }
    
    public Job getJob(int id) {
    	return jobMap.get(id);
    }
    
    public void removeJob(int id) {
    	jobMap.remove(id);
    }
    
    public void removeWorkerHandler(WorkerHandler wh) {
    	workerHandlerList.remove(wh);
    }
    
    public void removeClientHandler(ClientHandler ch) {
    	clientHandlerList.remove(ch);
    }
    
    public void addMapJob(MapJob job) {
    	mapJobList.add(job);
    }
    
    public void addReduceJob(ReduceJob job) {
    	reduceJobList.add(job);
    }
    
    public void removeMapJob(MapJob job) {
    	mapJobList.remove(job);
    }
    
    public void removeReduceJob(ReduceJob job) {
    	reduceJobList.remove(job);
    }

    public ReduceJob getReduceJob(int jobID) {
        for (ReduceJob job : reduceJobList)
            if (job.getID() == jobID)
                return job;
        return null;
    }

    public ReducePartition getReducePartition(int jobID,
            int partitionIdx) {
        ReduceJob job = getReduceJob(jobID);
        if (job != null)
            return job.getPartition(partitionIdx);
        return null;
    }

    public MapJob getMapJob(int jobID) {
        for (MapJob job : mapJobList)
            if (job.getID() == jobID)
                return job;
        return null;
    }

    public MapJobSplit getMapJobSplit(int jobID, int splitIdx) {
        MapJob job = getMapJob(jobID);
        if (job != null)
            return job.getSplit(splitIdx);
        return null;
    }

    private boolean initMap(WorkerHandler wh, MapJobSplit split,
            int jobID, int splitIdx) {
        try {
			wh.writeObject(new Signal(SigNum.INIT_MAP));
			wh.writeObject(getJob(jobID));
			wh.writeObject(split.getInputSplit());
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
        split.setJobState(JobState.IN_PROGRESS);
        split.setWorkerID(wh.getID());
        wh.setWorkerState(WorkerState.BUSY);
        wh.setJobStatus(WorkerHandler.MAP_JOB, jobID, splitIdx);
        return true;
    }

    private boolean initReduce(WorkerHandler wh, ReducePartition partition,
            int jobID, int partitionIdx) {
        try {
        	wh.writeObject(new Signal(SigNum.INIT_REDUCE));
        	wh.writeObject(getJob(jobID));
			wh.writeObject(partition);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
        partition.setJobState(JobState.IN_PROGRESS);
        partition.setWorkerID(wh.getID());
        wh.setWorkerState(WorkerState.BUSY);
        wh.setJobStatus(WorkerHandler.REDUCE_JOB, jobID, partitionIdx);
        return true;
    }

    public void migrate(MapJob mapJob) {
    	System.out.println("Migrating map job " + mapJob.getID() + " to reduce job list...");
        MapJobSplit[] splits = mapJob.getSplits();
        if (splits.length == 0)
            return;
        ReducePartition[] partitions = new ReducePartition[Configuration.NUM_OF_REDUCERS];
        for (int i = 0; i < Configuration.NUM_OF_REDUCERS; i++) {
            // Partition i
            ArrayList<Integer> mapperIDList = new ArrayList<Integer>();
            ArrayList<String> filenameList = new ArrayList<String>();
            for (MapJobSplit split : splits) {
                String filename = split.getIntermediateFilename(i);
                if (filename != null) {
                    mapperIDList.add(split.getWorkerID());
                    filenameList.add(filename);
                }
            }
            Integer[] mIDs = mapperIDList.toArray(new Integer[mapperIDList.size()]);
            int[] mapperIDs = new int[mIDs.length];
            for (int j = 0; j < mapperIDs.length; j++)
                mapperIDs[j] = mIDs[j];
            partitions[i] = new ReducePartition(mapperIDs,
                filenameList.toArray(new String[filenameList.size()]));
        }
        ReduceJob reduceJob = new ReduceJob(mapJob.getID(), partitions);
        removeMapJob(mapJob);
        addReduceJob(reduceJob);
        System.out.println("Map job " + mapJob.getID() + "migrated to reduce job list.");
    }

    private void advanceMapIdx() {
        if (!hasMapJob())
            curMapJobIdx = 0;
        else
            curMapJobIdx = (curMapJobIdx + 1) % mapJobList.size();
    }

    private void advanceReduceIdx() {
        if (!hasReduceJob())
            curReduceJobIdx = 0;
        else
            curReduceJobIdx = (curReduceJobIdx + 1) % reduceJobList.size();
    }

    private MapJob getCurrentMapJob() {
        return mapJobList.get(curMapJobIdx);
    }

    private ReduceJob getCurrentReduceJob() {
        return reduceJobList.get(curReduceJobIdx);
    }

    private boolean hasMapJob() {
        return mapJobList.size() > 0;
    }

    private boolean hasReduceJob() {
        return reduceJobList.size() > 0;
    }

    private boolean hasJob() {
        return hasMapJob() && hasReduceJob();
    }

    private void toNextJob() {
        if (shouldDoMap)
            advanceMapIdx();
        else
            advanceReduceIdx();
        shouldDoMap ^= true;
    }

    private WorkerHandler getWorkerHandler(Host host) {
        for (WorkerHandler wh : workerHandlerList)
            if (wh.getSocket().getLocalAddress().equals(host.getIPAddress()))
                return wh;
        return null;
    }

    private JobTracker getThis() {
        return this;
    }

    // Check whether workers are alive
    private class HeartbeatThread extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
//            	System.out.println("Worker handler list size = " + workerHandlerList.size());
                for (int i = 0; i < workerHandlerList.size(); i++) {
                	WorkerHandler wh = workerHandlerList.get(i);
//                	System.out.print("Worker handler #" + wh.getID() + " is ");
//                	if (wh.alive())
//                		System.out.println("alive.");
//                	else
//                		System.out.println("not alive.");
                    if (!wh.alive()) {
                        // Worker Failure
                    	System.out.println("Worker (" + wh.getSocket().getLocalAddress() + ") failed.");
                    	workerHandlerList.remove(i--);
                    }
                }
                try {
                    Thread.sleep(Configuration.TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientListener extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                Socket socket = null;
                try {
                    socket = clientServerSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(getThis(), nextClientID++, socket);
                    clientHandlerList.add(clientHandler);
                    clientHandler.start();
                    System.out.println("New client connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect client: " + socket.getRemoteSocketAddress());
                    e.printStackTrace();
                }
            }
        }
    }

    private class WorkerListener extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                Socket socket = null;
                try {
                    socket = workerServerSocket.accept();
                    WorkerHandler workerHandler = new WorkerHandler(getThis(), nextWorkerID++, socket);
                    workerHandlerList.add(workerHandler);
                    workerHandler.start();
                    System.out.println("New worker connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect worker: " + socket.getRemoteSocketAddress());
                    e.printStackTrace();
                }
            }
        }
    }
    
    public static void main(String[] args) {
    	try {
    		JobTracker jobTracker = new JobTracker();
    		jobTracker.start();
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }
}