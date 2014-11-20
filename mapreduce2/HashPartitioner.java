package mapreduce2;


public class HashPartitioner<Key, Value> implements Partitioner<Key, Value> {
	
	public int getPartition(Key key, Value value, int numPartitions) {
		return Math.abs(key.hashCode()) % numPartitions;
	}
}
