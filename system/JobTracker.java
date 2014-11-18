import java.util.ArrayList;
import java.util.HashMap;
import java.net.ServerSocket;
import java.net.Socket;

public class JobTracker {
    private ArrayList<WorkerHandler> workerHandlerList;
    private ArrayList<ClientHandler> clientHandlerList;

    private ArrayList<MapJob> mapJobList;
    private ArrayList<ReduceJob> reduceJobList;

    private int nextClientId;
    private int nextWorkerId;

    // Round Robin between map and reduce, and between jobs
    private int curMapJobIdx, curReduceJobIdx;
    private boolean doMap;

    private boolean running;

    private ServerSocket clientServerSocket, workerServerSocket;

    private Configuration conf;

    public JobTracker(Configuration config) {
        nextClientId = 0;
        nextWorkerId = 0;
        running = true;
        curMapJobIdx = curReduceJobIdx = 0;
        doMap = true;

        conf = config;

        workerHandlerList = new ArrayList<WorkerHandler>();
        clientHandlerList = new ArrayList<ClientHandler>();
        mapJobList = new ArrayList<MapJob>();
        reduceJobList = new ArrayList<ReduceJob>();
        clientServerSocket = new ServerSocket(conf.getMaster().getClientPort());
        workerServerSocket = new ServerSocket(conf.getMaster().getWorkerPort());

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
                                if (split.getState() != JobState.COMPLETED)
                                    mapCompleted = false;
                                if (split.getState() != JobState.IDLE)
                                    continue;
                                Host[] hosts = split.getInputSplit.getLocations();
                                for (Host host : hosts) {
                                    WorkerHandler wh = getWorkerHandler(host);
                                    if (wh != null && wh.isIdle()) { // Found idle worker
                                        initMap(wh, split, job.getId(), i);
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
                                        initReduce(wh, partition, job.getId(), idx);
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

    private void initMap(WorkerHandler wh, MapJobSplit split,
            int jobId, int splitIdx) {
        wh.writeObject(new Signal(Signal.INIT_MAP));
        wh.writeObject(split.getInputSplit());
        split.setState(JobState.IN_PROGRESS);
        wh.setState(WorkerState.BUSY);
        wh.setJobStatus(WorkerHandler.MAP_JOB, jobId, splitIdx);
    }

    private void initReduce(WorkerHandler wh, ReducePartition partition,
            int jobId, int partitionIdx) {

    }

    private void migrate(MapJob mapJob) {
        MapJobSplit[] splits = mapJob.getSplits();
        if (splits.length == 0)
            return;
        
        ReducePartition[] partitions;
        ReduceJob reduceJob = new ReduceJob(mapJob.getId(), partitions);
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
            if (wh.getSocket().getLocalAddress().equals(host.getAddress()))
                return wh;
        return null;
    }

    // Check whether workers are alive
    private class HeartbeatThread extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                for (WorkerHandler wh : workerHandlerList)
                    if (!wh.isAlive()) {
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
                try {
                    Socket socket = clientServerSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(conf, nextClientId++, socket);
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
                try {
                    Socket socket = workerServerSocket.accept();
                    WorkerHandler workerHandler = new WorkerHandler(conf, nextWorkerId++, socket);
                    workerHandlerList.add(workerHandler);
                    System.out.println("New worker connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect worker: " + socket.getRemoteSocketAddress());
                }
            }
        }
    }
}