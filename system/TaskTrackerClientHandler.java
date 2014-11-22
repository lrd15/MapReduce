package system;

import java.io.*;
import java.net.Socket;

import config.Configuration;

public class TaskTrackerClientHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    boolean running;

    public TaskTrackerClientHandler(int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        running = true;
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
        	toClient = new ObjectOutputStream(socket.getOutputStream());
        	fromClient = new ObjectInputStream(socket.getInputStream());
        	while (running) {
				Object obj = fromClient.readObject();
				if (obj instanceof Signal) {
                    Signal sig = (Signal)obj;
                    switch (sig.getSignal()) {
                    	case SEND_SPLIT:
                    		System.out.println("Receiving input split...");
                    		String filename = (String)fromClient.readObject();
                    		System.out.println("Received filename: " + filename);
                    		FileOutputStream fos = new FileOutputStream(
                    				new File(JobTracker.INPUT_DIR + File.separator + filename));
                    		while (true) {
                    			Object subObj = fromClient.readObject();
                    			if (subObj instanceof Integer) {
                					int bytesRead = (Integer)subObj;
                					byte[] buffer = (byte[])fromClient.readObject();
                					System.out.println("Bytes received: " + bytesRead);

                					if (fos == null)
                						System.out.println("FileOutputStream is null pointer.");
                					
                					// Write bytes to file
                					fos.write(buffer, 0, bytesRead);
                				}
                    			else {
                    				if (subObj instanceof Signal) {
                    					if (((Signal)subObj).getSignal() == SigNum.SEND_SPLIT_COMPLETED) {
		                					fos.close();
		            						System.out.println("Input split received. Sending ACK..");
		            						toClient.writeObject(new Signal(SigNum.SPLIT_RECEIVED));
		            						System.out.println("ACK sent.");
                    					}
                    					else
                    						System.out.println("Unexpected signal received: " + ((Signal)subObj).getSignal());
                    				}
                    				else
                    					System.out.println("Unexpected object received.");
                    				break;
                    			}
                    		}
                    		break;
                    	case SEND_FILE_COMPLETED:
                    		System.out.println("All file splits sent. Session ended.");
                    		toClient.writeObject(new Signal(SigNum.SESSION_ENDED));
                    		break;
						default:
							System.out.println("Unexpected signal received: " + sig.getSignal());
							break;
                    }
				}
			}
        } catch (IOException e) {
        	e.printStackTrace();
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}