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
            
        } catch (IOException e) {
        	e.printStackTrace();
        }
    }
}