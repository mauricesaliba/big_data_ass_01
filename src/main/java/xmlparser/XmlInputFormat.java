package xmlparser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
 
public class XmlInputFormat extends TextInputFormat {
    
	private static Logger logger = Logger.getLogger("XMLPARSER_02");
	public static String CONST_START_TAG_KEY;
    public static String CONST_END_TAG_KEY;
 

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }
 
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[] startTag;
        private byte[] endTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();
 
        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac)
                throws IOException, InterruptedException {
        	Configuration conf = tac.getConfiguration();
        	StringBuilder sb1 = new StringBuilder("<");
        	String elementName = conf.get("elementName");    	
        	sb1.append(elementName);
        	//sb1.append(">");
        	CONST_START_TAG_KEY = sb1.toString();
        	StringBuilder sb2 = new StringBuilder("</");   	
        	sb2.append(elementName);
        	sb2.append(">");
        	CONST_END_TAG_KEY = sb2.toString();
        	//logger.info("CONST_START_TAG_KEY: " + CONST_START_TAG_KEY); 
        	//logger.info("CONST_END_TAG_KEY: " + CONST_END_TAG_KEY); 
                    	
        	FileSplit fileSplit = (FileSplit) is;
            String START_TAG_KEY = CONST_START_TAG_KEY;
            String END_TAG_KEY = CONST_END_TAG_KEY;            

            startTag = START_TAG_KEY.getBytes("utf-8");
            endTag = END_TAG_KEY.getBytes("utf-8");
 
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
 
            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
 
        }
 
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
 
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }
 
        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }
 
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
 
        }
 
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }
 
        @Override
        public void close() throws IOException {
            fsin.close();
        }
 
        //if end tag or self-closing tag is read return true end finish buffer writing. 
        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
            int i = 0;
            boolean selfClosingTagPossible = true;
            boolean forwardSlashFound = false;
            boolean closingBracketFound = false;
            
            while (true) {
                int b = fsin.read();                
                if (b == -1)
                    return false;
 
                if (withinBlock)
                    buffer.write(b);
                
                //String s = new String(buffer.getData());
                //logger.info(s);
                
                if (b == match[i]) {
                    i++;
                    if ( i >= match.length ) //i == closing angle bracket >
                        return true;
                } else
                    i = 0;
                
                //checking if self closing tag
                if(selfClosingTagPossible){
	                //forward slash ('/') found
	                if(b==47) forwardSlashFound = true;
                }
	            
                
                //checking if self closing tag
                if(selfClosingTagPossible){
	                //closing bracket ('>') found 
	                if(b==62){
	                	closingBracketFound = true;               
	                
	                	if(forwardSlashFound && closingBracketFound){ 
	                		//tag is self-closing	                		
	                		return true;
	                	}
	                	else{
	                		//then endtag is required to end reading
	                		selfClosingTagPossible = false;
	                	}
	                }
                }
 
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
        }
 
    }
}