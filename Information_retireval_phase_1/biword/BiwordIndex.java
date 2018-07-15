package biword;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


public class BiwordIndex {

    public static class BiwordIndexMap extends Mapper<Text,BytesWritable,Text,Text>{

    	private Text value = new Text();
        private Text wordText=new Text();
        public void map( Text file_key, BytesWritable byte_value, Context infoContex )
            throws IOException
            {
            	String filename = file_key.toString();
            	if ( filename.endsWith(".xml") == false )
                return;
            
	        try{    

	            String rawtext = new String( byte_value.getBytes(), "UTF-8" );
	            String info = new String();
	            Stopwords stopstr = new Stopwords();

	            DocumentBuilderFactory docbuildFactory = DocumentBuilderFactory.newInstance();

				DocumentBuilder docBuilder = docbuildFactory.newDocumentBuilder();
				Document doc = docBuilder.parse( new InputSource( new StringReader(rawtext) ) );
				doc.getDocumentElement().normalize();

				NodeList nodeList = doc.getElementsByTagName("text");
                
			   	StringBuilder infoPage = new StringBuilder(1024);
				for (int ind = 0; ind < nodeList.getLength(); ind++) 
				{
					Node node = nodeList.item(ind);
					if (node.getNodeType() == Node.ELEMENT_NODE) {

						Element element = (Element) node;
						NodeList ptagElements = element.getElementsByTagName("p");

			 			for (int i = 0; i < ptagElements.getLength(); i++) {

			 				infoPage.append(ptagElements.item(i).getTextContent());
						}

					}

				}
				info = infoPage.toString();
				info = info.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
				info = stopstr.removeStemmedStopWords(info);
				info = stopstr.stopWordsRemover(info);

	            StringTokenizer strTokenizer = new StringTokenizer( info );
	        	String currenttoken = strTokenizer.nextToken();

	            while (strTokenizer.hasMoreTokens()) {

	            	String comingtoken = strTokenizer.nextToken();
                   	wordText.set( currenttoken + " " + comingtoken +":"+ filename);
	                value.set("1");
	                infoContex.write(wordText, value);
	                currenttoken = comingtoken;
	            }  
	        }
	        catch (Exception e) {
            	e.printStackTrace();

           }
        }  
    } 
    
    public static class BiwordIndexCombiner extends Reducer<Text,Text,Text,Text>{

        Text infoText = new Text();    
        public void reduce(Text key, Iterable<Text> values,Context infoContex)

            throws IOException, InterruptedException {
            int total = 0;        
            for (Text indexValue : values) {
            	total += Integer.parseInt(indexValue.toString());
            }

            int indexSplit = key.toString().indexOf(":");
            infoText.set(key.toString().substring(indexSplit+1) +":"+ total);
            key.set(key.toString().substring(0,indexSplit));
            infoContex.write(key, infoText);

        }
    }

    public static class BiwordIndexReduce extends Reducer<Text,Text,Text,Text>{

        private Text resultText = new Text();
        public void reduce(Text key, Iterable<Text> values,Context contex)
	                throws IOException, InterruptedException {

		StringBuffer fileBuffer = new StringBuffer();
		for(Text value : values) {
			fileBuffer.append(value.toString()+";") ;
		} 
		resultText.set(fileBuffer.toString());
		contex.write(key, resultText);
		}

    }


/*

    public static class UniwordReduce extends Reducer<Text,Text,Text,Text>{

        

        private Text result = new Text();

        

        public void reduce(Text key, Iterable<Text> values,Context contex)

	                throws IOException, InterruptedException {

		StringBuffer fileList = new StringBuffer();

		for(Text value : values) {

			fileList.append(value.toString()+";") ;

		} 

		result.set(fileList.toString());

		contex.write(key, result);

		}

    }

*/

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
        Job job = new Job(conf,"Uniword");
        job.setJarByClass(BiwordIndex.class);
        job.setMapperClass(BiwordIndexMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(BiwordIndexCombiner.class);
        job.setReducerClass(BiwordIndexReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(ZipFileInput.class);
        ZipFileInput.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
