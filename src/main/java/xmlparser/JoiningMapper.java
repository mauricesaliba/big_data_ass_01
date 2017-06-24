package xmlparser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

    private int keyIndex;
    private Splitter splitter;
    private Joiner joiner;
    private TaggedKey taggedKey = new TaggedKey();
    private Text data = new Text();
    private int joinOrder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"));
        String separator = context.getConfiguration().get("separator");
        splitter = Splitter.on(separator).trimResults();
        joiner = Joiner.on(separator);
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(splitter.split(value.toString()));
        String joinKey = values.remove(keyIndex);
        String valuesWithOutKey = joiner.join(values);
        taggedKey.set(joinKey, joinOrder);
        data.set(valuesWithOutKey);
        context.write(taggedKey, data);
    }

}