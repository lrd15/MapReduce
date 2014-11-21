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

    public TaskTrackerWorkerHandler(int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        running = true;
        this.socket = socket;
        
    }

    @Override
    public void run() {
       	try {
       		toWorker = new ObjectOutputStream(socket.getOutputStream());
			fromWorker = new ObjectInputStream(socket.getInputStream());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}