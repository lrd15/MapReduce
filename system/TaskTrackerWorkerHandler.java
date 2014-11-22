package system;

import java.io.*;
import java.net.Socket;

import config.Configuration;

public class TaskTrackerWorkerHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromWorker;
    private ObjectOutputStream toWorker;

    private boolean running;
    private TaskTracker taskTracker;

    public TaskTrackerWorkerHandler(TaskTracker taskTracker, int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        running = true;
        this.socket = socket;
        this.taskTracker = taskTracker;
    }

    @Override
    public void run() {
       	try {
       		toWorker = new ObjectOutputStream(socket.getOutputStream());
			fromWorker = new ObjectInputStream(socket.getInputStream());
			while (running) {
				Object obj = fromWorker.readObject();
				if (obj instanceof Signal) {
                    Signal sig = (Signal)obj;
                    switch (sig.getSignal()) {
		                case RETRIEVE_FILE:
		                	Object filenameObj = fromWorker.readObject();
		                	if (filenameObj instanceof String) {
		                		String filename = (String)filenameObj;
		                		try {
		                			FileInputStream fis = new FileInputStream(new File(JobTracker.MAPOUT_DIR + File.separator + filename));
		                			toWorker.writeObject(new Signal(SigNum.SEND_SPLIT));
//		                			byte[] buffer = new byte[1024]; // 8KB
		                			int bytesRead;
//		                			while ((bytesRead = fis.read(buffer)) != -1) {
		                			while ((bytesRead = fis.read()) != -1) {
//		                				if (bytesRead > 0) {
		                				System.out.println("Bytes sent: " + bytesRead);
		                					toWorker.writeObject(new Integer(bytesRead));
//		                					toWorker.writeObject(buffer);
//		                				}
		                			}
		                			fis.close();
		                			toWorker.writeObject(new Signal(SigNum.SEND_SPLIT_COMPLETED));
		                			running = false;
		                		} catch (FileNotFoundException e) {
		                			e.printStackTrace();
		                			toWorker.writeObject(new Signal(SigNum.FILE_NOT_FOUND));
		                		} catch (Exception e) {
		                			e.printStackTrace();
		                			toWorker.writeObject(new Signal(SigNum.UNKNOWN));
		                		}
		                	}
		                	else {
		                		System.out.println("Unexpected object received.");
		                		toWorker.writeObject(new Signal(SigNum.UNKNOWN));
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       	taskTracker.removeWorkerHandler(this);
    }
}