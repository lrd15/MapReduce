package system;

import java.io.*;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

import lib.input.InputSplit;
import config.Configuration;

public class TaskTracker extends Thread {
    private ArrayList<TaskTrackerWorkerHandler> workerHandlerList;
    private ArrayList<TaskTrackerClientHandler> clientHandlerList;

    private Socket socket;
    private ObjectInputStream fromHandler;
    private ObjectOutputStream toHandler;

    private int nextClientID;
    private int nextWorkerID;

    private boolean running;

    private ServerSocket clientServerSocket, workerServerSocket;

    public TaskTracker(String ipAddress) throws Exception {
        nextClientID = 0;
        nextWorkerID = 0;
        running = true;
        Host self = Configuration.getWorkerByAddress(ipAddress);
        if (self == null)
            throw new Exception("This host is not registered.");

        clientServerSocket = new ServerSocket(self.getPortForClient());
        workerServerSocket = new ServerSocket(self.getPortForWorker());

        Host master = Configuration.MASTER;
        socket = new Socket(master.getIPAddress(), master.getPortForWorker());

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
        
        try {
			fromHandler = new ObjectInputStream(socket.getInputStream());
			toHandler = new ObjectOutputStream(socket.getOutputStream());
			while (running) {
				Object obj = fromHandler.readObject();
				if (obj instanceof Signal) {
	                Signal sig = (Signal)obj;
	                switch (sig.getSignal()) {
	                    case INIT_MAP:
	                    	System.out.println("New map request coming...");
	                        Object splitObj = fromHandler.readObject();
	                        if (splitObj instanceof InputSplit) {
	                            InputSplit inputSplit = (InputSplit)splitObj;
	                            boolean success = doMap(inputSplit);
	                            if (success) {
	                            	System.out.println("Map operation completed.");
	                                toHandler.writeObject(new Signal(SigNum.MAP_COMPLETED));
	                                // Send object "String[conf.NUM_OF_REDUCERS] filenames" - abby
	                            }
	                            else {
	                            	System.out.println("Map operation failed.");
	                            }
	                        }
	                        break;
	                    case INIT_REDUCE:
	                        // Code here
	                    	System.out.println("New reduce request coming...");
	                    	Object partitionObj = fromHandler.readObject();
							if (partitionObj instanceof ReducePartition) {
	                            ReducePartition partition = (ReducePartition)partitionObj;
	                            boolean success = doReduce(partition);
	                            if (success) {
	                            	System.out.println("Reduce operation completed.");
	                                toHandler.writeObject(new Signal(SigNum.REDUCE_COMPLETED));
	                            }
	                            else {
	                            	System.out.println("Reduce operation failed.");
	                            }
	                        }
	                        break;
	                    default:
	                    	System.out.println("Unexpected signal received: " + sig.getSignal());
	                    	break;
	                }
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }

    // Return true if successful
    private boolean doMap(InputSplit inputSplit) {
        // TODO by abby
    	return true;
    }

    // Return true if successful
    private boolean doReduce(ReducePartition partition) {
        // TODO by abby
    	return true;
    }

    // Periodically send heartbeat to job tracker
    private class HeartbeatThread extends Thread {
        @Override
        synchronized public void run() {
            while (running) {       
                try {
                	System.out.println("Sending heartbeat...");
                	toHandler.writeObject(new Signal(SigNum.HEARTBEAT));
                    Thread.sleep(Configuration.TIMEOUT / 2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
					// TODO Auto-generated catch block
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
                    TaskTrackerClientHandler clientHandler = new TaskTrackerClientHandler(nextClientID++, socket);
                    clientHandlerList.add(clientHandler);
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
                    TaskTrackerWorkerHandler workerHandler = new TaskTrackerWorkerHandler(nextWorkerID++, socket);
                    workerHandlerList.add(workerHandler);
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
    		if (args.length != 1) {
    			System.out.println("Usage: TaskTracker <ip_address>");
    			return;
    		}
    		TaskTracker taskTracker = new TaskTracker(args[0]);
    		taskTracker.start();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}