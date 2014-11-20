package system;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.ServerSocket;
import java.net.Socket;

import config.Configuration;

public class JobTracker extends Thread {
    private ArrayList<WorkerHandler> workerHandlerList;
    private ArrayList<ClientHandler> clientHandlerList;

    private ArrayList<MapJob> mapJobList;
    private ArrayList<ReduceJob> reduceJobList;

    private int nextClientID;
    private int nextWorkerID;

    // Round Robin between map and reduce, and between jobs
    private int curMapJobIdx, curReduceJobIdx;
    private boolean doMap;

    private boolean running;

    private ServerSocket clientServerSocket, workerServerSocket;

    public JobTracker() throws IOException {
        nextClientID = 0;
        nextWorkerID = 0;
        running = true;
        curMapJobIdx = curReduceJobIdx = 0;
        doMap = true;

        workerHandlerList = new ArrayList<WorkerHandler>();
        clientHandlerList = new ArrayList<ClientHandler>();
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
        while (running) {
            // Start a separate thread to check if each worker is alive
            HeartbeatThread heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();

            while (running) {
                while (hasJob()) {
                    boolean done = false;
                    if (doMap) {
                        if (hasMapJob()) {
                            MapJob job = getCurrentMapJob();
                            MapJobSplit[] splits = job.getSplits();
                            boolean mapCompleted = true;
                            for (int i = 0; i < splits.length; i++) {
                                MapJobSplit split = splits[i];
                                if (split.getJobState() != JobState.COMPLETED)
                                    mapCompleted = false;
                                if (split.getJobState() != JobState.IDLE)
                                    continue;
                                Host[] hosts = split.getInputSplit().getLocations();
                                for (Host host : hosts) {
                                    WorkerHandler wh = getWorkerHandler(host);
                                    if (wh != null && wh.isIdle()) { // Found idle worker
                                        initMap(wh, split, job.getID(), i);
                                        done = true;
                                        break;
                                    }
                                }
                                if (done)
                                    break;
                            }
                            if (mapCompleted) {
                                migrate(job); // Migrate job from mapJobList to reduceJobList
                                mapJobList.remove(job);
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
                                        initReduce(wh, partition, job.getID(), idx);
                                        done = true;
                                        break;
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
    }

    public MapJobSplit getMapJobSplit(int jobID, int splitIdx) {
        for (MapJob job : mapJobList)
            if (job.getID() == jobID)
                return job.getSplit(splitIdx);
        return null;
    }

    private void initMap(WorkerHandler wh, MapJobSplit split,
            int jobID, int splitIdx) {
        wh.writeObject(new Signal(Signal.INIT_MAP));
        wh.writeObject(split.getInputSplit());
        split.setJobState(JobState.IN_PROGRESS);
        split.setWorkerID(wh.getID());
        wh.setWorkerState(WorkerState.BUSY);
        wh.setJobStatus(WorkerHandler.MAP_JOB, jobID, splitIdx);
    }

    private void initReduce(WorkerHandler wh, ReducePartition partition,
            int jobID, int partitionIdx) {
        wh.writeObject(new Signal(Signal.INIT_REDUCE));
        wh.writeObject(partition);
        partition.setJobState(JobState.IN_PROGRESS);
        partition.setWorkerID(wh.getID());
        wh.setWorkerState(WorkerState.BUSY);
        wh.setJobStatus(WorkerHandler.REDUCE_JOB, jobID, partitionIdx);
    }

    private void migrate(MapJob mapJob) {
        MapJobSplit[] splits = mapJob.getSplits();
        if (splits.length == 0)
            return;
        ReducePartition[] partitions = new ReducePartition[conf.NUM_OF_REDUCERS];
        for (int i = 0; i < conf.NUM_OF_REDUCERS; i++) {
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
            for (int i = 0; i < mapperIDs.length; i++)
                mapperIDs[i] = mIDs[i];
            partitions[i] = new ReducePartition(mapperIDs,
                filenameList.toArray(new String[filenameList.size()]));
        }
        ReduceJob reduceJob = new ReduceJob(mapJob.getID(), partitions);
        reduceJobList.add(reduceJob);
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
        if (doMap)
            advanceMapIdx();
        else
            advanceReduceIdx();
        doMap ^= true;
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
                for (WorkerHandler wh : workerHandlerList)
                    if (!wh.alive()) {
                        // Worker Failure
                    }
                try {
                    Thread.sleep(conf.TIMEOUT);
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
                Socket socket;
                try {
                    socket = clientServerSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(getThis(), conf, nextClientID++, socket);
                    clientHandlerList.add(clientHandler);
                    System.out.println("New client connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect client: " + socket.getRemoteSocketAddress());
                }
            }
        }
    }

    private class WorkerListener extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                Socket socket;
                try {
                    socket = workerServerSocket.accept();
                    WorkerHandler workerHandler = new WorkerHandler(getThis(), conf, nextWorkerID++, socket);
                    workerHandlerList.add(workerHandler);
                    System.out.println("New worker connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect worker: " + socket.getRemoteSocketAddress());
                }
            }
        }
    }
}