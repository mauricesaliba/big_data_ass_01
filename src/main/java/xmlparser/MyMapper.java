package xmlparser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
 
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
 
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private static Logger logger = Logger.getLogger("XMLPARSER_01");
    private static String ELEMENT_NAME;
   
    /*
    public void configure(JobConf job) {
    	ELEMENT_NAME = job.get("elementName");
    }
    */    
    public void setup(Context context){
    	Configuration config = context.getConfiguration();
    	ELEMENT_NAME = config.get("elementName");
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        try {
 
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);
 
            doc.getDocumentElement().normalize();
 
            NodeList nList = doc.getElementsByTagName(ELEMENT_NAME);
        	logger.info("ELEMENT_NAME: " + ELEMENT_NAME);
        	
        	 //logger.info("nList.getLength() >>>>>>>>>>>: " + nList.getLength());
        	
            for (int temp = 0; temp < nList.getLength(); temp++) {
 
                Node nNode = nList.item(temp);
 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 
                    Element eElement = (Element) nNode;
 
                    String id = eElement.getAttribute("id");
                    //logger.info("ELEMENT_>>>>>>>>>>>: " + id);
                    String lat = eElement.getAttribute("lat");
                    String lon = eElement.getAttribute("lon");
                    /*
                    String gender = eElement.getElementsByTagName("gender").item(0).getTextContent();
 					*/
           
                    context.write(new Text(id + "," + lat + "," + lon), NullWritable.get());
 
                }
            }
        } catch (Exception e) {
        	logger.error(e.getMessage());
        }
 
    }
 
}
