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
                    		// Store filename
                    		break;
                    	case SEND_SPLIT_COMPLETED:
                    		// Close fileoutputstream
                    		System.out.println("Input split received.");
                    		break;
                    	case SEND_FILE_COMPLETED:
                    		running = false;
                    		System.out.println("All file splits send. Session ended.");
                    		break;
						default:
							System.out.println("Unexpected signal received: " + sig.getSignal());
							break;
                    }
				}
				else if (obj instanceof Integer) {
					int numOfBytes = (Integer)obj;
					byte[] bytes = (byte[])fromClient.readObject();
					System.out.println(numOfBytes);
					// Write bytes to file
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