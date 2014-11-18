import java.net.InetAddress;
import java.util.ArrayList;
public class Configuration {
    private ArrayList<Host> participants;
    private Host master;

    public Configuration(Host master, ArrayList<Host> participants) {
        this.master = master;
        this.participants = new ArrayList<Host>(participants);
    }
}