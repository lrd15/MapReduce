import java.util.ArrayList;
import java.util.HashMap;
import java.net.ServerSocket;
import java.net.Socket;

public class JobTracker {
    private HashMap<Integer, JobSplit[]> mapJobList, reduceJobList;
    private ArrayList<WorkerHandler> workerHandlerList;
    private ArrayList<ClientHandler> clientHandlerList;

    private int nextClientId;
    private int nextWorkerId;
    private int curJobId, minJobId, maxJobId;

    private boolean running;

    private ServerSocket clientServerSocket, workerServerSocket;

    private Configuration conf;

    public JobTracker(Configuration config) {
        nextClientId = 0;
        nextWorkerId = 0;
        curJobId = 0;
        minJobId = 0;
        maxJobId = -1;
        running = true;

        conf = config;

        workerHandlerList = new ArrayList<WorkerHandler>();
        clientHandlerList = new ArrayList<ClientHandler>();
        mapJobList = new HashMap<Integer, JobSplit[]>();
        reduceJobList = new HashMap<Integer, JobSplit[]>();
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
                    boolean map = true;
                    boolean mapCompleted = true;
                    JobSplit[] jobSplits = getCurrentMapJobSplits();
                    if (jobSplits == null) {
                        jobSplits = getCurrentReduceJobSplits();
                        if (jobSplits == null) { // This job id does not exist
                            toNextJob();
                            continue;
                        }
                        map = false; // reduce operation
                    }

                    for (JobSplit split : jobSplits) {
                        if (map && split.getState() != State.COMPLETED)
                            mapCompleted = false;
                        if (map)
                        Host[] hosts = split.getInputSplit().getLocations();
                        for (Host host : hosts) {
                            WorkerHandler wh = getWorkerHandler(host);
                            if (wh != null && wh.isIdle()) { // Found idle worker
                                // Assign
                            }
                        }
                    }

                    if (map && mapCompleted) { // Transfer job from mapJobList to reduceJobList

                    }
                }
            }
        }
    }

    private JobSplit[] getCurrentMapJobSplits() {
        return mapJobList.get(curJobId);
    }

    private JobSplit[] getCurrentReduceJobSplits() {
        return reduceJobList.get(curJobId);
    }

    private boolean hasJob() {
        return maxJobId >= minJobId;
    }

    public void toNextJob() {
        if (++curJobId > maxJobId)
            curJobId = minJobId;
    }

    public WorkerHandler getWorkerHandler(Host host) {
        for (WorkerHandler wh : workerHandlerList)
            if (wh.getSocket().getLocalAddress().equals(host.getAddress()))
                return wh;
        return null;
    }

    private class JobSplit {
        InputSplit split;
        State state;
        int workerId;
        String[] intermediateFilenames;

        public JobSplit(InputSplit split) {
            this.split = split;
            state = State.IDLE;
            workerId = -1;
            intermediateFilenames = null;
        }

        public void setIntermediateFilenames(String[] filenames) {
            int n = filenames.length;
            intermediateFilenames = new String[n];
            for (int i = 0; i < n; i++)
                intermediateFilenames[i] = filenames[i];
        }

        public State getState() {
            return state;
        }

        public void setState(State s) {
            state = s;
        }

        public int getWorkerId() {
            return workerId;
        }

        public void setWorkerId(int id) {
            workerId = id;
        }

        public InputSplit getInputSplit() {
            return split;
        }
    }

    private enum State {
        IDLE, IN_PROGRESS, COMPLETED
    }

    // Check whether workers are alive
    private class HeartbeatThread extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                for (WorkerHandler wh : workerHandlerList)
                    if (!wh.isAlive()) {
                        // kill worker and transfer work
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