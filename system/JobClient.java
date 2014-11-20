package system;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	
	public void submitJob(Job job) {
		this.job = job;
		getJobIDFromMaster();
		sendFilesToWorkers();
		sendInputSplitsToMaster();
		acknowledgeMaster();
	}
	
	private void getJobIDFromMaster() {
		try {
			toMaster.writeObject(new Signal(SigNum.ADD_JOB));
			int jobID = (Integer)fromMaster.readObject();
			this.job.setID(jobID);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}	
	}
	
	private void sendInputSplitsToMaster() {
		int numSplits = Configuration.NUM_OF_SPLITS;
		InputFormat inputFormat = null;
		InputSplit[] inputSplits = null;
		try {
			inputFormat = (InputFormat)job.getInputFormatClass().newInstance();
			inputSplits = inputFormat.getSplits(job, numSplits);
			toMaster.writeObject(inputSplits);
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendFilesToWorkers() {
		ArrayList<ObjectOutputStream> toWorkers = new ArrayList<ObjectOutputStream>();
		ArrayList<ObjectInputStream> fromWorkers = new ArrayList<ObjectInputStream>();
		for(Host worker : Configuration.WORKERS) {
			Socket socket;
			try {
				socket = new Socket(worker.getIPAddress(), worker.getPortForClient());
				toWorkers.add(new ObjectOutputStream(socket.getOutputStream()));
				fromWorkers.add(new ObjectInputStream(socket.getInputStream()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//send files
	}
	
	private void acknowledgeMaster() {
		try {
			toMaster.writeObject(new Signal(SigNum.ADD_JOB_COMPLETED));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
