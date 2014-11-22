package system;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.ArrayList;

import lib.input.FileInputSplit;
import lib.input.InputSplit;
import config.Configuration;
import config.Job;

public class JobClient {

	private Job job;
	private ArrayList<InputSplit> inputSplits;
	private ObjectOutputStream toMaster;
	private ObjectInputStream fromMaster;

	public JobClient() {
		this.inputSplits = new ArrayList<InputSplit>();
		Host master = Configuration.MASTER;
		try {
			Socket socket = new Socket(master.getIPAddress(), master.getPortForClient());
			toMaster = new ObjectOutputStream(socket.getOutputStream());
			fromMaster = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void submitJob(Job job) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		System.out.println("Job submitted");
		this.job = job;
		getJobIDFromMaster();
		sendFilesToWorkers();
		sendJobContextToMaster();
		acknowledgeMaster();
		sendInputSplitsToMaster();
	}
	
	private void getJobIDFromMaster() throws IOException, ClassNotFoundException {
		toMaster.writeObject(new Signal(SigNum.ADD_JOB));
		int jobID = (Integer) fromMaster.readObject();
		this.job.setID(jobID);
	}

	private void sendInputSplitsToMaster() throws InstantiationException, IllegalAccessException, IOException {
		toMaster.writeObject(this.inputSplits.toArray());
	}

	private void sendFilesToWorkers() throws IOException, ClassNotFoundException  {
		ArrayList<ObjectOutputStream> toWorkers = new ArrayList<ObjectOutputStream>();
		ArrayList<ObjectInputStream> fromWorkers = new ArrayList<ObjectInputStream>();
		for (Host worker : Configuration.WORKERS) {
			Socket socket = null;
			try {
				socket = new Socket(worker.getIPAddress(), worker.getPortForClient());
			} catch (IOException e) {
				System.err.print("Worker "+worker.getIPAddress().getHostAddress() + " is not connected.");
				continue;
			}
			toWorkers.add(new ObjectOutputStream(socket.getOutputStream()));
			fromWorkers.add(new ObjectInputStream(socket.getInputStream()));
		}

		File folder = job.getInputPath();
		if(!folder.isDirectory()) {
			throw new IOException("The input path is not a directory");
		}
		splitAndSend(folder.listFiles(), toWorkers, fromWorkers);
		
		for(int i=0; i < toWorkers.size(); i++) {
			toWorkers.get(i).writeObject(new Signal(SigNum.SEND_FILE_COMPLETED));
			fromWorkers.get(i).readObject(); //SESSION_ENDED
			toWorkers.get(i).close();
			fromWorkers.get(i).close();
		}
	}

	private void sendJobContextToMaster() throws IOException {
		toMaster.writeObject(new Signal(SigNum.SEND_JOB_CONTEXT));
		toMaster.writeObject(this.job); //JobContext
	}
	
	private void acknowledgeMaster() throws IOException {
		toMaster.writeObject(new Signal(SigNum.ADD_JOB_COMPLETED));
	}

	private void splitAndSend(File[] files, ArrayList<ObjectOutputStream> toWorkers,
										    ArrayList<ObjectInputStream> fromWorkers) throws IOException, ClassNotFoundException {
		int ptr = 0;
		int numOfWorker = toWorkers.size();
		int numSplits = Configuration.NUM_OF_SPLITS;
		long maxReadBufferSize = 8 * 1024; //8KB
		
		//loop through files
		for(File file : files) {
			String filename = this.job.getInputPath() + File.separator + file.getName();
			RandomAccessFile inputFile = new RandomAccessFile(filename, "r");
			long sourceSize = inputFile.length();
			long bytesPerSplit = sourceSize / numSplits;
			long remainingBytes = sourceSize % numSplits;
			//loop through splits
			for (int spl = 0; spl < numSplits; spl++) {
				String splitFilename = job.getID() + "_" + spl + "_" + file.getName();
				ObjectOutputStream oos = toWorkers.get(ptr);
 				ObjectInputStream ois = fromWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_SPLIT));
 				oos.writeObject(splitFilename);
 				if (bytesPerSplit > maxReadBufferSize) {
 					long numReads = bytesPerSplit / maxReadBufferSize;
 					long numRemainingRead = bytesPerSplit % maxReadBufferSize;
 					for (int i = 0; i < numReads; i++) {
 						readWrite(inputFile, oos, maxReadBufferSize);
 					}
 					if (numRemainingRead > 0) {
 						readWrite(inputFile, oos, numRemainingRead);
 					}
 				} else {
 					readWrite(inputFile, oos, bytesPerSplit);
 				}
				oos.writeObject(new Signal(SigNum.SEND_SPLIT_COMPLETED));
 				Object obj = ois.readObject();
 				if(obj instanceof Signal) {
 					Signal sig = (Signal)obj;
 					if(sig.getSignal() != SigNum.SPLIT_RECEIVED) {
 						System.err.println("wrong signal");
 					}
 				} else {
 					System.err.println("wrong object");
 				}
 				InputSplit thisSplit = new FileInputSplit(splitFilename, 0, bytesPerSplit);
 				this.inputSplits.add(thisSplit);
				ptr = (ptr+1) % numOfWorker; 
 			}
 			if (remainingBytes > 0) {
 				String splitFilename = job.getID() + "_" + numSplits + "_" + file.getName();
 				ObjectOutputStream oos = toWorkers.get(ptr);
 				ObjectInputStream ois = fromWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_SPLIT));
 				oos.writeObject(splitFilename);
 				readWrite(inputFile, oos, remainingBytes);
				oos.writeObject(new Signal(SigNum.SEND_SPLIT_COMPLETED));
				Object obj = ois.readObject();
 				if(obj instanceof Signal) {
 					Signal sig = (Signal)obj;
 					if(sig.getSignal() != SigNum.SPLIT_RECEIVED) {
 						System.err.println("wrong signal");
 					}
 				} else {
 					System.err.println("wrong object");
 				}
 				InputSplit thisSplit = new FileInputSplit(splitFilename, 0, remainingBytes);
 				this.inputSplits.add(thisSplit);
				ptr = (ptr+1) % numOfWorker;
 			}
			
			inputFile.close();
		}
	}

	private void readWrite(RandomAccessFile raf, ObjectOutputStream oos, long numBytes) throws IOException {
 		byte[] buf = new byte[(int) numBytes];
 		int bytesRead = raf.read(buf);
 		if (bytesRead != -1) {
 			oos.writeObject(new Integer(bytesRead));
 			oos.writeObject(buf);
 		}
 	}
}
