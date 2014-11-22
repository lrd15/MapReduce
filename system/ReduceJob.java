package system;

public class ReduceJob {
    int id;
    int numCompleted;
    ReducePartition[] partitions;

    public ReduceJob(int id, ReducePartition[] par) {
        this.id = id;
        numCompleted = 0;
        partitions = new ReducePartition[par.length];
        for (int i =  0; i < par.length; i++)
            partitions[i] = new ReducePartition(par[i]);
    }

    public int getID() {
        return id;
    }

    public boolean isCompleted() {
        return numCompleted == partitions.length;
    }

    synchronized public void incNumCompleted() {
        numCompleted++;
    }
    
    public ReducePartition[] getPartitions() {
    	return partitions;
    }

    public ReducePartition getPartition(int i) {
        if (i >= partitions.length)
            return null;
        return partitions[i];
    }
}