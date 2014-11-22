package system;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
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
		//send job object
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

	private void sendFilesToWorkers() throws IOException  {
		System.out.println("JobClient sending files to workers");
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
		File[] files = folder.listFiles();
		splitAndSend(files, toWorkers, fromWorkers);
		
		//TODO
		
		for(int i=0; i < toWorkers.size(); i++) {
			//TODO toWorkers.get(i).writeObject(new Signal(SigNum.SEND_FILE_COMPLETED));
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
		long numSplits = Configuration.NUM_OF_SPLITS;
		long maxReadBufferSize = 8 * 1024; //8KB
		
		//loop through files
		for(File file : files) {
			String filename = this.job.getInputPath() + File.separator + file.getName();
			RandomAccessFile inputFile = new RandomAccessFile(filename, "r");
			System.out.println("Sending file: " + filename);
			long sourceSize = inputFile.length();
			long bytesPerSplit = sourceSize / numSplits;
			long remainingBytes = sourceSize % numSplits;
			//loop through splits
			for (int destIx = 1; destIx <= numSplits; destIx++) {
				System.out.println("Sending split " + destIx + " to worker " + ptr);
				ObjectOutputStream oos = toWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_SPLIT));
				oos.writeObject(file.getName()+destIx);
				
				long n = bytesPerSplit;
				while (n > 0) {
					long bytesToWrite = Math.min(n, maxReadBufferSize);
					int bytesWritten = readWrite(inputFile, oos, bytesToWrite);
					if (bytesWritten == -1)
						break;
					n -= bytesWritten;
				}
				
//				while(readWrite(inputFile, oos, maxReadBufferSize)); //keep sending 
				ptr = (ptr+1) % numOfWorker; //TODO
			}
			if (remainingBytes > 0) {
				System.out.println("Sending split " + (numSplits+1) + " to worker " + ptr);
				ObjectOutputStream oos = toWorkers.get(ptr);
				oos.writeObject(new Signal(SigNum.SEND_SPLIT));
				oos.writeObject(file.getName()+(numSplits+1));
				readWrite(inputFile, oos, remainingBytes);
				ptr = (ptr+1) % numOfWorker;
			}
			
			inputFile.close();
		}
	}

	private int readWrite(RandomAccessFile raf, ObjectOutputStream oos, long numBytes) throws IOException {
		byte[] buf = new byte[(int) numBytes];
		int bytesRead = raf.read(buf);
		oos.writeObject(new Integer(bytesRead));
		if (bytesRead != -1)
			oos.writeObject(buf);
		return bytesRead;
	}
}
