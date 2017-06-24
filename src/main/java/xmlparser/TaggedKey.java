package xmlparser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
       return compareValue;
    }
   
    
	@Override
	public void readFields(DataInput arg0) throws IOException {
		//TODO - auto-generated		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		//TODO - auto-generated				
	}


	public Text getJoinKey() {
		return joinKey;
	}


	public void setJoinKey(Text joinKey) {
		this.joinKey = joinKey;
	}


	public IntWritable getTag() {
		return tag;
	}


	public void setTag(IntWritable tag) {
		this.tag = tag;
	}
 }