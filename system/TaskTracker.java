package system;

import java.io.*;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;

import lib.input.*;
import lib.output.*;
import mapreduce2.*;
import config.*;

public class TaskTracker extends Thread {
	private ArrayList<TaskTrackerWorkerHandler> workerHandlerList;
	private ArrayList<TaskTrackerClientHandler> clientHandlerList;

	private Socket socket;
	private ObjectInputStream fromHandler;
	private ObjectOutputStream toHandler;

	private int nextClientID;
	private int nextWorkerID;

	private boolean running;
	private String ipAddress;

	private ServerSocket clientServerSocket, workerServerSocket;

	public TaskTracker(String ipAddress) throws Exception {

		new File(JobTracker.MAPIN_DIR).mkdirs();
		new File(JobTracker.MAPOUT_DIR).mkdirs();
		new File(JobTracker.REDUCEIN_DIR).mkdirs();

		nextClientID = 0;
		nextWorkerID = 0;
		running = true;
		this.ipAddress = ipAddress;
		Host self = Configuration.getWorkerByAddress(ipAddress);
		if (self == null)
			throw new Exception("This host is not registered.");

		clientServerSocket = new ServerSocket(self.getPortForClient());
		workerServerSocket = new ServerSocket(self.getPortForWorker());

		clientHandlerList = new ArrayList<TaskTrackerClientHandler>();
		workerHandlerList = new ArrayList<TaskTrackerWorkerHandler>();

		Host master = Configuration.MASTER;
		socket = new Socket(master.getIPAddress(), master.getPortForWorker());
		System.out.println("Connected to Job Tracker ("
				+ master.getIPAddress().getHostAddress() + ":"
				+ master.getPortForWorker() + ")");

		ClientListener clientListener = new ClientListener();
		clientListener.start();
		WorkerListener workerListener = new WorkerListener();
		workerListener.start();
	}

	@Override
	public void run() {
		System.out.println("Task tracker (" + ipAddress + ") running...");
		try {
			toHandler = new ObjectOutputStream(socket.getOutputStream());
			fromHandler = new ObjectInputStream(socket.getInputStream());

			// Start a separate thread to periodically send heartbeat to job
			// tracker
			HeartbeatThread heartbeatThread = new HeartbeatThread();
			heartbeatThread.start();

			while (running) {
				Object obj = fromHandler.readObject();
				if (obj instanceof Signal) {
					Signal sig = (Signal) obj;
					Job job = null;
					boolean success = false;
					switch (sig.getSignal()) {
					case INIT_MAP:
						System.out.println("New map request coming...");
						job = (Job) fromHandler.readObject();
						InputSplit inputSplit = (InputSplit) fromHandler
								.readObject();
						String[] filenames = new String[Configuration.NUM_OF_REDUCERS];
						success = doMap(job, inputSplit, filenames);
						if (success) {
							System.out.println("Map operation completed.");
							toHandler.writeObject(new Signal(
									SigNum.MAP_COMPLETED));
							toHandler.writeObject(filenames);
						} else {
							System.out.println("Map operation failed.");
						}
						break;
					case INIT_REDUCE:
						System.out.println("New reduce request coming...");
						job = (Job) fromHandler.readObject();
						ReducePartition partition = (ReducePartition) fromHandler
								.readObject();
						success = doReduce(job, partition);
						if (success) {
							System.out.println("Reduce operation completed.");
							toHandler.writeObject(new Signal(
									SigNum.REDUCE_COMPLETED));
						} else {
							System.out.println("Reduce operation failed.");
						}
						break;
					default:
						System.out.println("Unexpected signal received: "
								+ sig.getSignal());
						break;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Return true if successful
	private boolean doMap(Job job, InputSplit inputSplit, String[] filenames) {
		try {
			InputFormat inputFormat = (InputFormat) job.getInputFormatClass().newInstance();
			Partitioner partitioner = (Partitioner) job.getPartitionerClass().newInstance();
			RecordReader<Long, String> reader = inputFormat.getRecordReader(job, JobTracker.MAPIN_DIR, inputSplit);
			filenames = inputFormat.getFilenames();
			MapContext mapContext = new MapContext<Long, String, String, String>(job, reader, partitioner);
			Mapper mapper = (Mapper) job.getMapperClass().newInstance();
			mapper.run(mapContext);
		} catch (InstantiationException e) {
			e.printStackTrace();
			return false;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// Return true if successful
	private boolean doReduce(Job job, ReducePartition partition) {
		int[] mapperIDs = partition.getMapperIDs();
		String[] filenames = partition.getFilenames();

		// TODO
		try {
			OutputFormat outputFormat = (OutputFormat) job.getOutputFormatClass().newInstance();
			ArrayList<LineRecordReader> readers = new ArrayList<LineRecordReader>();
			for (int i = 0; i < filenames.length; i++) {
				LineRecordReader reader = new LineRecordReader(JobTracker.REDUCEIN_DIR + filenames[i]);
				readers.add(reader);
			}
	
			StringStringIterator itr = new StringStringIterator(readers);
			RecordWriter writer = outputFormat.getRecordWriter(job);
			ReduceContext reduceContext = new ReduceContext<String, String, String, String>(
					job, itr, writer);
			Reducer reducer = (Reducer) job.getReducerClass().newInstance();
			reducer.run(reduceContext);
		} catch (InstantiationException e) {
			e.printStackTrace();
			return false;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// Periodically send heartbeat to job tracker
	private class HeartbeatThread extends Thread {
		@Override
		public void run() {
			System.out.println("Task tracker heartbeat thread started.");
			while (running) {
				try {
					// System.out.println("Sending heartbeat...");
					toHandler.writeObject(new Signal(SigNum.HEARTBEAT));
					// System.out.println("Heartbeat sent");
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
					TaskTrackerClientHandler clientHandler = new TaskTrackerClientHandler(
							nextClientID++, socket);
					clientHandlerList.add(clientHandler);
					clientHandler.start();
					System.out.println("New client connected: "
							+ socket.getRemoteSocketAddress());
				} catch (IOException e) {
					System.err.println("Failed to connect client: "
							+ socket.getRemoteSocketAddress());
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
					TaskTrackerWorkerHandler workerHandler = new TaskTrackerWorkerHandler(
							nextWorkerID++, socket);
					workerHandlerList.add(workerHandler);
					workerHandler.start();
					System.out.println("New worker connected: "
							+ socket.getRemoteSocketAddress());
				} catch (IOException e) {
					System.err.println("Failed to connect worker: "
							+ socket.getRemoteSocketAddress());
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