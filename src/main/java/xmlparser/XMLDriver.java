package xmlparser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/* as adapted from tutorials on the internet on XMLInputFormat brought from
 * Mahout. This did not support self closing tags.
 */

public class XMLDriver {
 

    public static void main(String[] args) {
        try {
 
            Configuration conf = new Configuration();
            String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
            
            String elementNameToParse =  args[2];
 
            conf.set("elementName", elementNameToParse);
            
            Job job = Job.getInstance(conf,"XML Processing");
            job.setJarByClass(XMLDriver.class);
            
            switch(elementNameToParse)
            {
	            case "node":
	            	job.setMapperClass(NodeMapper.class);
	            	break;
	            case "way":
	            	job.setMapperClass(WayMapper.class);
	            	break;
	        	default:
	        		break;            	
            }
 
            job.setNumReduceTasks(0); 
            job.setInputFormatClass(XmlInputFormat.class);
 
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
 
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
 
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
            job.waitForCompletion(true);
 
        } catch (Exception e) {
            System.out.println(e.getMessage().toString());
        } 
    }
 
}
