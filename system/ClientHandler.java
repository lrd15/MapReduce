package system;

import java.io.*;
import java.net.Socket;

import lib.input.InputSplit;
import config.Job;

public class ClientHandler extends Thread {
    private int id;
    private ObjectInputStream fromClient;
    private ObjectOutputStream toClient;

    private JobTracker master;
    private boolean running;
    private Socket socket;

    public ClientHandler(JobTracker master, int id, Socket socket) throws IOException {
        this.master = master;
//        System.out.println("ClientHandler constructor master: " + master.hashCode());
//        System.out.println("ClientHandler constructor this.master: " + this.master.hashCode());
        this.id = id;
        this.socket = socket;
        running = true;
    }

    @Override
    public void run() {
    	System.out.println("Client handler #" + id + " running...");
    	try {
    		toClient = new ObjectOutputStream(socket.getOutputStream());
	    	fromClient = new ObjectInputStream(socket.getInputStream());
	        
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
		                    case SEND_JOB_CONTEXT:
		                    	System.out.println("Receiving job object...");
		                    	Job job = (Job)fromClient.readObject();
		                    	master.addJob(job);
		                    	System.out.println("Job object received.");
		                    	break;
		                    case ADD_JOB_COMPLETED:
		                    	System.out.println("Splits sending completed.");
		                    	InputSplit[] splits = (InputSplit[])fromClient.readObject();
	                            MapJob mapJob = new MapJob(id, splits);
//	                            System.out.println("ClientHandler: " + master.hashCode());
	                            master.addMapJob(mapJob);
//	                            if (master.hasMapJob())
//	                            	System.out.println("-----------------------");
	                            running = false; // End this session
	                            System.out.println(splits.length + " input splits received. Client session ended.");
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