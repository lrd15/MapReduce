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

    public ClientHandler(JobTracker master, int id, Socket socket) throws IOException {
        this.master = master;
        this.id = id;
        running = true;
        fromClient = new ObjectInputStream(socket.getInputStream());
        toClient = new ObjectOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        while (running) {
			try {
				Object obj = fromClient.readObject();
				if (obj instanceof Signal) {
	                Signal sig = (Signal)obj;
	                switch (sig.getSignal()) {
	                    case ADD_JOB:
	                        toClient.writeObject(new Integer(id));
	                        break;
	                    case ADD_JOB_COMPLETED:
	                        Object splitsObj = fromClient.readObject();
	                        if (splitsObj instanceof InputSplit[]) {
	                            InputSplit[] splits = (InputSplit[])splitsObj;
	                            MapJob job = new MapJob(id, splits);
	                            master.addMapJob(job);
	                            running = false; // End this session
	                        }
	                        break;
	                    default:
	                    	break;
	                }
	            }
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
            
        }
        master.removeClientHandler(this);
    }
}