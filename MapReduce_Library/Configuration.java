import java.net.InetAddress;
import java.util.ArrayList;
public class Configuration {
    private int MAXNUM_MAP_PER_HOST = 100;
    private int MAXNUM_REDUCE_PER_HOST = 10;
    private ArrayList<Host> participants;
    private Host master;

    public Configuration(Host master, ArrayList<Host> participants) {
        this.master = master;
        this.participants = new ArrayList<Host>(participants);
    }

    public Configuration(Host master, ArrayList<Host> participants,
                int max_map, int max_reduce) {
        this(master, participants);
        MAXNUM_MAP_PER_HOST = max_map;
        MAXNUM_REDUCE_PER_HOST = max_reduce;
    }
}