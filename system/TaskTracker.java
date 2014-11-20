import java.io.*;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

public class TaskTracker extends Thread {
    private ArrayList<TaskTrackerWorkerHandler> workerHandlerList;
    private ArrayList<TaskTrackerClientHandler> clientHandlerList;

    private Socket socket;
    private ObjectInputStream fromHandler;
    private ObjectOutputStream toHandler;

    private int nextClientID;
    private int nextWorkerID;

    private boolean running;

    private Configuration conf;

    private ServerSocket clientServerSocket, workerServerSocket;

    public TaskTracker(Configuration conf) throws Exception {
        nextClientID = 0;
        nextWorkerID = 0;
        running = true;
        this.conf = conf;
        Host self = conf.getWorkerByAddress(InetAddress.getLocalHost().getHostAddress());
        if (self == null)
            throw new Exception("This host is not registered.");

        clientServerSocket = new ServerSocket(self.getPortForClient());
        workerServerSocket = new ServerSocket(self.getPortForWorker());


        socket = new Socket(conf.getMaster().getIPAddress(), conf.getMaster().getPortForWorker());
        fromHandler = new ObjectInputStream(socket.getInputStream());
        toHandler = new ObjectOutputStream(socket.getOutputStream());

        ClientListener clientListener = new ClientListener();
        clientListener.start();
        WorkerListener workerListener = new WorkerListener();
        workerListener.start();
    }

    @Override
    public void run() {
        // Start a separate thread to periodically send heartbeat to job tracker
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();

        while (running) {
            Object obj = fromHandler.readObject();
            if (obj instanceof Signal) {
                Signal sig = (Signal)obj;
                switch (sig.getSignal()) {
                    case Signal.INIT_MAP:
                        Object splitObj = fromHandler.readObject();
                        if (splitObj instanceof InputSplit) {
                            InputSplit inputSplit = (InputSplit)splitObj;
                            boolean success = doMap(inputSplit);
                            if (success) {
                                toHandler.writeObject(new Signal(Signal.MAP_COMPLETED));
                                // Send object "String[conf.NUM_OF_REDUCERS] filenames" - abby
                            }
                        }
                        else {
                            // error
                        }
                        break;
                    case Signal.INIT_REDUCE:
                        // Code here
                        Object partitionObj = fromHandler.readObject();
                        if (partitionObj instanceof ReducePartition) {
                            ReducePartition partition = (ReducePartition)partitionObj;
                            boolean success = doReduce(partition);
                            if (success) {
                                toHandler.writeObject(new Signal(Signal.REDUCE_COMPLETED));
                                // No other information to send
                            }
                        }
                        break;
                
                }
            }
        }
    }

    // Return true if successful
    private boolean doMap(InputSplit inputSplit) {
        // TODO by abby
    }

    // Return true if successful
    private boolean doReduce(ReducePartition partition) {
        // TODO by abby
    }

    // Periodically send heartbeat to job tracker
    private class HeartbeatThread extends Thread {
        @Override
        synchronized public void run() {
            while (running) {
                toHandler.writeObject(new Signal(Signal.HEARTBEAT));
                try {
                    Thread.sleep(conf.TIMEOUT / 2);
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
                    TaskTrackerClientHandler clientHandler = new TaskTrackerClientHandler(conf, nextClientID++, socket);
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
                    TaskTrackerWorkerHandler workerHandler = new TaskTrackerWorkerHandler(conf, nextWorkerID++, socket);
                    workerHandlerList.add(workerHandler);
                    System.out.println("New worker connected: " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.err.println("Failed to connect worker: " + socket.getRemoteSocketAddress());
                }
            }
        }
    }
}