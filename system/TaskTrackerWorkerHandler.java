package system;

import java.io.*;
import java.net.Socket;

import config.Configuration;

public class TaskTrackerWorkerHandler extends Thread {
    private int id;
    private Socket socket;
    private ObjectInputStream fromWorker;
    private ObjectOutputStream toWorker;

    private Configuration conf;

    private boolean running;

    public TaskTrackerWorkerHandler(Configuration conf, int id, Socket socket) {
        this.id = id;
        this.socket = socket;
        this.conf = conf;
        running = true;
        this.socket = socket;
        fromWorker = new ObjectInputStream(socket.getInputStream());
        toWorker = new ObjectOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        while (running) {
            Object obj = fromWorker.readObject();
            if (obj instanceof Signal) {
                Signal sig = (Signal)obj;
                switch (sig.getSignal()) {
                
                }
            }
        }
    }
}