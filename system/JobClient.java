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
		System.out.println("Job submitted");
		this.job = job;
		getJobIDFromMaster();
		sendFilesToWorkers();
		acknowledgeMaster();
		sendInputSplitsToMaster();
	}
	
	private void getJobIDFromMaster() throws IOException, ClassNotFoundException {
		System.out.println("JobClient getting ID from master");
		toMaster.writeObject(new Signal(SigNum.ADD_JOB));
		int jobID = (Integer) fromMaster.readObject();
		this.job.setID(jobID);
	}

	private void sendInputSplitsToMaster() throws InstantiationException, IllegalAccessException, IOException {
		System.out.println("JobClient sending input splits to master");
		int numSplits = Configuration.NUM_OF_SPLITS;
		InputFormat inputFormat = null;
		InputSplit[] inputSplits = null;
		inputFormat = (InputFormat) job.getInputFormatClass().newInstance();
		inputSplits = inputFormat.getSplits(job, numSplits);
		toMaster.writeObject(inputSplits);
	}

	private void sendFilesToWorkers() throws IOException {
		System.out.println("JobClient sending files to workers");
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
		
		for(int i=0; i < toWorkers.size(); i++) {
			toWorkers.get(i).close();
			fromWorkers.get(i).close();
		}
	}

	private void acknowledgeMaster() throws IOException {
		System.out.println("JobClient acknowledging master");
		toMaster.writeObject(new Signal(SigNum.ADD_JOB_COMPLETED));
	}

	private void splitAndSend(File[] files, ArrayList<ObjectOutputStream> toWorkers,
										    ArrayList<ObjectInputStream> fromWorkers) throws IOException {
		int ptr = 0;
		int numOfWorker = toWorkers.size();
		
		//loop through files
		for(File file : files) {
			System.out.println("Sending file: " + file.getName());
			String filename = this.job.getInputPath() + File.separator + file.getName();
			RandomAccessFile inputFile = new RandomAccessFile(filename, "r");
			long numSplits = Configuration.NUM_OF_SPLITS;
			long sourceSize = inputFile.length();
			long bytesPerSplit = sourceSize / numSplits;
			long remainingBytes = sourceSize % numSplits;
			int maxReadBufferSize = 8 * 1024; //8KB
			//loop through splits
			for (int destIx = 1; destIx <= numSplits; destIx++) {
				System.out.println("Sending split: " + destIx);
				ObjectOutputStream oos = toWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_FILE));
				oos.writeObject(file.getName());
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
				oos.writeObject(new Signal(SigNum.SEND_FILE_COMPLETED));
				ptr = (ptr+1) % numOfWorker; //TODO
			}
			if (remainingBytes > 0) {
				ObjectOutputStream oos = toWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_FILE));
				oos.writeObject(file.getName());
				readWrite(inputFile, oos, remainingBytes);
				oos.writeObject(new Signal(SigNum.SEND_FILE_COMPLETED));
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
