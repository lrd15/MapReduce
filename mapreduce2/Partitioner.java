package mapreduce2;

public interface Partitioner<Key, Value> {
	public int getPartition(Key key, Value value, int numPartitions);
}
