package system;

import java.io.*;
import java.net.Socket;

import lib.input.InputSplit;

public class ClientHandler extends Thread {
    private int id;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    private JobTracker master;
    private boolean running;
    private Socket socket;

    public ClientHandler(JobTracker master, int id, Socket socket) throws IOException {
        this.master = master;
        this.id = id;
        this.socket = socket;
        running = true;
    }

    @Override
    public void run() {
    	try {
	    	fromClient = new ObjectInputStream(socket.getInputStream());
	        toClient = new ObjectOutputStream(socket.getOutputStream());
	        while (running) {
					Object obj = fromClient.readObject();
					if (obj instanceof Signal) {
		                Signal sig = (Signal)obj;
		                switch (sig.getSignal()) {
		                    case ADD_JOB:
		                    	System.out.println("New job coming...");
		                        toClient.writeObject(new Integer(id));
		                        System.out.println("Assign job id: " + id);
		                        break;
		                    case ADD_JOB_COMPLETED:
		                    	System.out.println("Splits sending completed.");
		                        Object splitsObj = fromClient.readObject();
		                        if (splitsObj instanceof InputSplit[]) {
		                            InputSplit[] splits = (InputSplit[])splitsObj;
		                            MapJob job = new MapJob(id, splits);
		                            master.addMapJob(job);
		                            running = false; // End this session
		                            System.out.println("Input splits received. Client session ends.");
		                        }
		                        break;
		                    default:
		                    	System.out.println("Unexpected signal received: " + sig.getSignal());
		                    	break;
		                }
		            }
	        }
    	} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
        master.removeClientHandler(this);
    }
}