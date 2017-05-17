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
 
public class WayMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private static Logger logger = Logger.getLogger("XMLPARSER_01");
	// example how to use: //logger.info("ELEMENT_>>>>>>>>>>>: " + id);
    private static String ELEMENT_NAME;
   
   
    public void setup(Context context)
    {
    	Configuration config = context.getConfiguration();
    	ELEMENT_NAME = config.get("elementName");
    }    
    
    //TODO - to increase performance by using stax parser:
    //https://www.tutorialspoint.com/java_xml/java_stax_parse_document.htm
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    { 
        try 
        { 
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);
 
            doc.getDocumentElement().normalize(); 
            NodeList nList = doc.getElementsByTagName(ELEMENT_NAME);
            
            
        	/*TODO - implement as object oriented - create mapper per element type to be parse and use 
        	 * chained map reduce.
        	 * 
        	 * possible tags to parse are node or way. Subelement of node is tag and for
        	 * way there are nd elements which refer to nodes and it also has descriptive elements 
        	 * with name 'tag'.
        	 * 
        	 * algorithm for parsing. If 'node' element parse node id,lat and lon. Get nested 'tag' 
        	 * elements and include in same output key, value line.
        	 * 
        	 * if 'way' element parse id and get nested 'nd' elements and 'tag' elements. 
        	 * 
        	 */
            	
        	//format: wid,nid,[k|v:tags]
        	
            for (int i = 0; i < nList.getLength(); i++) 
            { 
                Node nNode = nList.item(i);
 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) 
                { 
                    Element eElement = (Element) nNode;
 
                    String id = eElement.getAttribute("id");
                    StringBuilder sb_tags = new StringBuilder();
                    
                    NodeList tagsList_2 = eElement.getElementsByTagName("tag");  //.item(0).getTextContent();                      
                    sb_tags.append("[");
                    
                    //iterate node tags
                    int tagsList_2_length = tagsList_2.getLength(); 
                    for (int j = 0; j < tagsList_2_length; j++) 
                    { 
                    	Node tagNode = tagsList_2.item(j);                        
                    	Element tagElement = (Element) tagNode;
                    	String k = tagElement.getAttribute("k");
                    	String v = tagElement.getAttribute("v");
                    	sb_tags.append(k);
                    	sb_tags.append("|");
                    	sb_tags.append(v);
                    	if(j + 1 != tagsList_2_length)
                    	{
                    		sb_tags.append("|");
                    	}                        	
                    }
                    sb_tags.append("]");                    
                      
                    NodeList nodeRefList = eElement.getElementsByTagName("nd");                      
                    //iterate node references
                    for (int j = 0; j < nodeRefList.getLength(); j++) 
                    { 
                    	Node nodeRef = nodeRefList.item(j);                        
                    	Element tagElement = (Element) nodeRef;
                    	String ref = tagElement.getAttribute("ref");
                        StringBuilder sb_line = new StringBuilder(id);
                        sb_line.append(",");
                    	sb_line.append(ref);
                    	sb_line.append(",");
                    	sb_line.append(sb_tags);
                    	//for each way/node do a map output
                    	context.write(new Text(sb_line.toString()), NullWritable.get());                    	                       	
                    }
                }
            }
        } 
        catch (Exception e) 
        {
        	logger.error(e.getMessage());
        } 
    } 
}
