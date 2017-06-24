package xmlparser;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class TaggedJoiningPartitioner extends Partitioner<TaggedKey,Text> {

    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}