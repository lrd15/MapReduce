package system;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;

import lib.input.InputFormat;
import lib.input.InputSplit;
import config.Configuration;
import config.Job;

public class JobClient {

	private Job job;
	private ObjectOutputStream toMaster;
	private ObjectInputStream fromMaster;

	public JobClient() {
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
		this.job = job;
		getJobIDFromMaster();
		sendFilesToWorkers();
		acknowledgeMaster();
		sendInputSplitsToMaster();
	}
	
	public void submitJobOnSingleNode(Job job) { }

	private void getJobIDFromMaster() throws IOException, ClassNotFoundException {
		toMaster.writeObject(new Signal(SigNum.ADD_JOB));
		int jobID = (Integer) fromMaster.readObject();
		this.job.setID(jobID);
	}

	private void sendInputSplitsToMaster() throws InstantiationException, IllegalAccessException, IOException {
		int numSplits = Configuration.NUM_OF_SPLITS;
		InputFormat inputFormat = null;
		InputSplit[] inputSplits = null;
		inputFormat = (InputFormat) job.getInputFormatClass().newInstance();
		inputSplits = inputFormat.getSplits(job, numSplits);
		toMaster.writeObject(inputSplits);
	}

	private void sendFilesToWorkers() throws IOException {
		ArrayList<ObjectOutputStream> toWorkers = new ArrayList<ObjectOutputStream>();
		ArrayList<ObjectInputStream> fromWorkers = new ArrayList<ObjectInputStream>();
		for (Host worker : Configuration.WORKERS) {
			Socket socket = new Socket(worker.getIPAddress(), worker.getPortForClient());
			toWorkers.add(new ObjectOutputStream(socket.getOutputStream()));
			fromWorkers.add(new ObjectInputStream(socket.getInputStream()));
		}
		Path inputPath = job.getInputPath();
		File folder = new File(inputPath.toUri());
		File[] files = folder.listFiles();
		splitAndSend(files, toWorkers, fromWorkers);
		for(ObjectOutputStream oos : toWorkers) {
			oos.close();
		}
		for(ObjectInputStream ois : fromWorkers) {
			ois.close();
		}
	}

	private void acknowledgeMaster() throws IOException {
		toMaster.writeObject(new Signal(SigNum.ADD_JOB_COMPLETED));
	}

	private void splitAndSend(File[] files, ArrayList<ObjectOutputStream> toWorkers,
										    ArrayList<ObjectInputStream> fromWorkers) throws IOException {
		int ptr = 0;
		int numOfWorker = toWorkers.size();
		
		for(File file : files) {
			String filename = file.getName();
			RandomAccessFile inputFile = new RandomAccessFile(filename, "r");
			long numSplits = Configuration.NUM_OF_SPLITS;
			long sourceSize = inputFile.length();
			long bytesPerSplit = sourceSize / numSplits;
			long remainingBytes = sourceSize % numSplits;
			int maxReadBufferSize = 64 * 1024; // 64KB
	
			for (int destIx = 1; destIx <= numSplits; destIx++) {
				ObjectOutputStream oos = toWorkers.get(ptr);
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
				ptr = (ptr+1) % numOfWorker;
			}
			if (remainingBytes > 0) {
				ObjectOutputStream oos = toWorkers.get(ptr);
				readWrite(inputFile, oos, remainingBytes);
				ptr = (ptr+1) % numOfWorker;
			}
			
			inputFile.close();
		}
	}

	private void readWrite(RandomAccessFile raf, ObjectOutputStream bw, long numBytes) throws IOException {
		byte[] buf = new byte[(int) numBytes];
		int val = raf.read(buf);
		if (val != -1) {
			bw.write(buf);
		}
	}
}
