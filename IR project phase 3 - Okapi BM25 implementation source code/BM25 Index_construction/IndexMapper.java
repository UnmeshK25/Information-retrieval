import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.BytesWritable;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import java.io.StringReader;

public class IndexMapper
	extends Mapper<Text, BytesWritable, Text, Text> 
{

	private Text keyword1 = new Text();
	private Text keyvalue1 = new Text();

	public void map( Text key, BytesWritable value, Context context )
	    throws IOException, InterruptedException 
	{
	    try{

		    String filename = key.toString();                     //Parsing xml files
		     if ( filename.endsWith(".xml") == false )
			 return;
		    
		    String content = new String( value.getBytes(), "UTF-8" );

		    DocumentBuilderFactory docBuildFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuildFactory.newDocumentBuilder();
			Document document = docBuilder.parse( new InputSource( new StringReader(content) ) );

			document.getDocumentElement().normalize();
			NodeList ndList = document.getElementsByTagName("text");
		   	StringBuilder stringPage = new StringBuilder(1024);

			for (int it = 0; it < ndList.getLength(); it++) 
			{
				Node node = ndList.item(it);
				if (node.getNodeType() == Node.ELEMENT_NODE) 
				{
					Element element = (Element) node;
					NodeList pTagElements = element.getElementsByTagName("p");
		 			for (int i = 0; i < pTagElements.getLength(); i++) 
		 			{
			 				stringPage.append(pTagElements.item(i).getTextContent() + " ");
					}
				}
			}

			content = stringPage.toString();
		    content = content.replaceAll( "[^A-Za-z \n]", " " ).toLowerCase();

		    content = Stopwords.removeStemmedStopWords(content);
		    String[] contentArray = content.split("\\s+");

		    for(int i = 0; i < contentArray.length; i++)
		    {
		    	String contentWord = contentArray[i];
		    	if(Stopwords.checkStopWord(contentWord) == true)
		    	{
		    		contentArray[i] = null;
		    	}
		    }

		    StringBuffer interResult = new StringBuffer();
			for (int i = 0; i < contentArray.length; i++) 
			{
				if(contentArray[i] != null){
					interResult.append( contentArray[i] );
					interResult.append(" ");
				}
			}
			content = interResult.toString();
			StringTokenizer strToken = new StringTokenizer(content);

		    while (strToken.hasMoreTokens()) 
		    {        	
          		 String stemStr = strToken.nextToken();
          		 keyword1.set( stemStr + "," +  key.toString().replace("newsML.xml","") );
	             keyvalue1.set("1");
	             context.write(keyword1, keyvalue1);
    
        	}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
