package system;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.InetAddress;
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
    
    public static final String MAPIN_DIR = "mapin";
    public static final String MAPOUT_DIR = "mapout";
    public static final String REDUCEIN_DIR = "reducein";

    public JobTracker() {
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
        
    }

    @Override
    public void run() {
    	try {
			clientServerSocket = new ServerSocket(Configuration.MASTER.getPortForClient());
			workerServerSocket = new ServerSocket(Configuration.MASTER.getPortForWorker());
		} catch (IOException e) {
			return;
		}
        

        ClientListener clientListener = new ClientListener();
        clientListener.start();
        WorkerListener workerListener = new WorkerListener();
        workerListener.start();
        
        // Start a separate thread to check if each worker is alive
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
        System.out.println("Heartbeat thread started.");
        
        while (running) {
//        	if (jobMap.size() > 0)
//        		System.out.println("# of Job = " + jobMap.size());
//        	if (mapJobList.size() > 0)
//        		System.out.println("# of MapJob = " + mapJobList.size());
//        	if (reduceJobList.size() > 0)
//        		System.out.println("# of ReduceJob = " + reduceJobList.size());
//        	boolean allBusy = true;
//        	boolean hasHandler = false;
//        	for (WorkerHandler wh : workerHandlerList) {
//        		hasHandler = true;
//        		if (wh.isIdle()) {
//        			allBusy = false;
//        			System.out.println("Worker (" + wh.getSocket().getInetAddress() + ") is idle...");
//        			break;
//        		}
//        	}
//        	if (hasHandler && allBusy)
//        		System.out.println("All workers busy...");
            if (hasJob()) {
//            	System.out.println("Has job...");
                boolean done = false;
                if (shouldDoMap) {
//                	System.out.println("Doing map next...");
                    if (hasMapJob()) {
                        MapJob job = getCurrentMapJob();
                        MapJobSplit[] splits = job.getSplits();
                        for (int i = 0; i < splits.length; i++) {
                            MapJobSplit split = splits[i];
                            if (split.getJobState() != JobState.IDLE)
                                continue;
                            System.out.println("Found idle map job split: JobID = " + job.getID() + ", SplitId = " + i);
							try {
								Host[] hosts = split.getInputSplit().getLocations();
								for (Host host : hosts) {
									System.out.println("Trying host (" + host.getIPAddress().getHostAddress() + ")...");
                                    WorkerHandler wh = getWorkerHandlerByHost(host);
                                    System.out.println("WorkerHandler (" + wh.getSocket().getInetAddress() + ") is " + wh.getWorkerState());
                                    if (wh != null && wh.isIdle()) { // Found idle worker
                                    	System.out.println("Initiating map operation...");
                                        done = initMap(wh, split, job.getID(), i);
                                        if (done) {
                                        	System.out.println("Worker (" + wh.getSocket().getInetAddress() +
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
//                	System.out.println("Doing reduce next...");
                    if (hasReduceJob()) {
                        ReduceJob job = getCurrentReduceJob();
                        ReducePartition[] partitions = job.getPartitions();
                        for (int i = 0; i < partitions.length; i++) {
                        	ReducePartition partition = partitions[i];
                        	if (partition.getJobState() != JobState.IDLE)
                        		continue;
//                        	System.out.println("Found idle reduce partition: JobID = " + job.getID() + ", PartitionIdx = " + i);
                        	for (WorkerHandler wh : workerHandlerList)
                                if (wh.isIdle()) { // Found idle worker
                                	System.out.println("Initiating reduce operation...");
                                    done = initReduce(wh, partition, job.getID(), i);
                                    if (done) {
                                    	System.out.println("Worker (" + wh.getSocket().getInetAddress() +
                                    			") assigned for reduce operation: JobID = " + job.getID() + ", PartitionId = " + i);
                                    	break;
                                    }
                                    else
                                    	System.out.println("Reduce operation initiation failed.");
                                }
                        	if (done)
                                break;
                        }
                    }
                }
                toNextJob();
            }
            try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
    }
    
    synchronized public void addJob(Job job) {
    	jobMap.put(job.getID(), job);
    	System.out.println("Job #" + job.getID() + " added.");
    }
    
    public Job getJob(int id) {
    	return jobMap.get(id);
    }
    
    synchronized public void removeJob(int id) {
    	jobMap.remove(id);
    	System.out.println("Job #" + id + " removed.");
    }
    
    synchronized public void removeWorkerHandler(WorkerHandler wh) {
    	workerHandlerList.remove(wh);
    }
    
    synchronized public void removeClientHandler(ClientHandler ch) {
    	clientHandlerList.remove(ch);
    }
    
    synchronized public void addMapJob(MapJob job) {
    	mapJobList.add(job);
    	System.out.println("MapJob #" + job.getID() + " added.");
    }
    
    synchronized public void addReduceJob(ReduceJob job) {
    	reduceJobList.add(job);
    	System.out.println("ReduceJob #" + job.getID() + " added.");
    }
    
    synchronized public void removeMapJob(MapJob job) {
    	mapJobList.remove(job);
    	System.out.println("MapJob #" + job.getID() + " removed.");
    }
    
    synchronized public void removeReduceJob(ReduceJob job) {
    	reduceJobList.remove(job);
    	System.out.println("ReduceJob #" + job.getID() + " removed.");
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

    synchronized private boolean initMap(WorkerHandler wh, MapJobSplit split,
            int jobID, int splitIdx) {
        try {
			wh.writeObject(new Signal(SigNum.INIT_MAP));
			wh.writeObject(new Integer(splitIdx));
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

    synchronized private boolean initReduce(WorkerHandler wh, ReducePartition partition,
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

    synchronized public void migrate(MapJob mapJob) {
    	System.out.println("Migrating map job " + mapJob.getID() + " to reduce job list...");
        MapJobSplit[] splits = mapJob.getSplits();
        if (splits.length == 0)
            return;
        ReducePartition[] partitions = new ReducePartition[Configuration.NUM_OF_REDUCERS];
        for (int i = 0; i < Configuration.NUM_OF_REDUCERS; i++) {
            // Partition i
            ArrayList<InetAddress> mapperIPList = new ArrayList<InetAddress>();
            ArrayList<String> filenameList = new ArrayList<String>();
            for (MapJobSplit split : splits) {
                String filename = split.getIntermediateFilename(i);
                if (filename != null) {
                    mapperIPList.add(getWorkerAddressByID(split.getWorkerID()));
                    filenameList.add(filename);
                }
            }
            InetAddress[] mapperIPs = mapperIPList.toArray(new InetAddress[mapperIPList.size()]);
            partitions[i] = new ReducePartition(mapperIPs,
                filenameList.toArray(new String[filenameList.size()]));
        }
        ReduceJob reduceJob = new ReduceJob(mapJob.getID(), partitions);
        removeMapJob(mapJob);
        addReduceJob(reduceJob);
        System.out.println("Map job " + mapJob.getID() + "migrated to reduce job list.");
    }
    
    private InetAddress getWorkerAddressByID(int workerID) {
    	for (WorkerHandler wh : workerHandlerList)
    		if (wh.getID() == workerID)
    			return wh.getSocket().getInetAddress();
    	return null;
    }

    synchronized private void advanceMapIdx() {
        if (!hasMapJob())
            curMapJobIdx = 0;
        else
            curMapJobIdx = (curMapJobIdx + 1) % mapJobList.size();
    }

    synchronized private void advanceReduceIdx() {
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

    public boolean hasMapJob() {
        return mapJobList.size() > 0;
    }

    private boolean hasReduceJob() {
        return reduceJobList.size() > 0;
    }

    private boolean hasJob() {
        return hasMapJob() || hasReduceJob();
    }

    synchronized private void toNextJob() {
        if (shouldDoMap)
            advanceMapIdx();
        else
            advanceReduceIdx();
        shouldDoMap ^= true;
    }

    private WorkerHandler getWorkerHandlerByHost(Host host) {
    	System.out.println("Trying to get worker handler by host...");
    	System.out.println("Host ip: " + host.getIPAddress().getHostAddress());
        for (WorkerHandler wh : workerHandlerList) {
        	System.out.println("WorkerHandler ip: " + wh.getSocket().getInetAddress().getHostAddress());
            if (wh.getSocket().getInetAddress().getHostAddress().equals(host.getIPAddress().getHostAddress()))
                return wh;
        }
        return null;
    }
    
    private JobTracker getThis() {
//    	System.out.println("getThis(): " + this.hashCode());
        return this;
    }
    
    synchronized public void killWorkerHandler(WorkerHandler wh) {
    	// If wh is doing MAP_JOB
    	if (wh.getJobStatus() == WorkerHandler.MAP_JOB) {
    		int jobID = wh.getJobID();
    		int splitID = wh.getIdx();
    		for (MapJob job : mapJobList)
    			if (job.getID() == jobID)
    				job.getSplit(splitID).setJobState(JobState.IDLE);
    	}
    	// If wh is doing REDUCE_JOB
    	if (wh.getJobStatus() == WorkerHandler.REDUCE_JOB) {
    		int jobID = wh.getJobID();
    		int partitionIdx = wh.getIdx();
    		for (ReduceJob job : reduceJobList)
    			if (job.getID() == jobID)
    				job.getPartition(partitionIdx).setJobState(JobState.IDLE);
    	}
    	
    	// If wh has completed some map job splits
    	// -> redo because intermediate files are
    	// no longer accessible
    	
    }

    // Check whether workers are alive
    private class HeartbeatThread extends Thread {
        @Override
        public void run() {
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
                    	System.out.println("Worker (" + wh.getSocket().getInetAddress() + ") failed.");
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
//    	try {
    		JobTracker jobTracker = new JobTracker();
//    		try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    		jobTracker.hashCode();
//    		System.out.println("main: " + jobTracker.hashCode());
    		jobTracker.start();
//    	} catch (IOException e) {
//    		e.printStackTrace();
//    	}
    }
}