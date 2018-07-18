package RR;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class RankedIR {
 
	//Hashmap declaration 
	public static HashMap<String, Integer> userQuery = new HashMap<String,Integer>(); //string is key term present in both query and index file 
	public static HashMap<String, Double> doctfidf = new HashMap<String,Double>(); //string is file name and double is tf-idf of term and documents
	
    public static class RankedRetrievalMapping extends Mapper<Object,Text,Text,Text>
    {
    
    	public void map(Object indexkey, Text indexcontent, Context mappingcontext) //key is the index file name and value is the contents of index file
    	throws IOException, InterruptedException
        {
        	try{  
        		String[] indexcontentsline = indexcontent.toString().split("\t");
        
            	Set<String> set = userQuery.keySet();
            	if(set.contains(indexcontentsline[0]))  //checking of whether query term present in index file
            	{
            		Text querytermkey = new Text(indexcontentsline[0]);
            		Text newsfilename = new Text(indexcontentsline[1]);
            		mappingcontext.write(querytermkey,newsfilename);
            	}
            }
        	
            catch (Exception e) {
            e.printStackTrace();
            }
        }  
    } 

    
    public static class RankRetrievalCmb extends Reducer<Text,Text,Text,Text>
    {
    	public void reduce(Text commonword, Iterable<Text> wordvalues,Context contex) //commonword is present in both query and document i.e index file and wordvalue is the values of commonwords
        throws IOException, InterruptedException
    	{
        	
        	int N = 2100963;  //n is the total number of documents in the collection i.e. uniwords(terms) in index file
        
        	for(Text value:wordvalues)
        	{       		
        		String indexdocumentCollection[] = value.toString().split(";");  //indexdocumentCollection is collection array of postings from posting list seperated by ';'
  
        		for(String indexdocumentToken:indexdocumentCollection)
        		{
        			String indexposDocument[] = indexdocumentToken.split(":");  
        		
        			String indexsep[]= indexposDocument[0].split("$");
        			Double indexdocsquare = Double.parseDouble(indexsep[1]);
        			
        			Double queryTF= (1+Math.log10(userQuery.get(commonword.toString())));  //tf computation of query terms
        			Double querytf_idf = queryTF * (Math.log10(N/indexdocumentCollection.length)); //tf-idf computation of query terms
        			Double documentTF = 1+Math.log10(Double.valueOf(indexposDocument[1])); //tf computation of documents in collection
        			
        			Double normanizeTF_IDF = documentTF/indexdocsquare; 
        			Double finaltf_idf = querytf_idf*normanizeTF_IDF; //final tf_idf after normalization
        			
        			
        			if(doctfidf.containsKey(indexsep[0]))
        				doctfidf.put(indexsep[0], finaltf_idf);
					else
						doctfidf.put(indexsep[0], finaltf_idf);

        		}
        	}
 
        	//Ranking of results through sorting based on tf-tdf scores
        	List<Map.Entry<String, Double>> doctf_idf_keys = new ArrayList<Map.Entry<String, Double>>(doctfidf.entrySet());
        		Collections.sort(doctf_idf_keys, new Comparator<Map.Entry<String, Double>>() 
        		{  
        			public int compare(Map.Entry<String, Double> entry1,Map.Entry<String, Double> entry2) 
        			{  
        				double tf_idfvalue1=Double.parseDouble(entry1.getValue().toString());
        				double tf_idfvalue2=Double.parseDouble(entry2.getValue().toString());
        				
        				BigDecimal tf_idf_data1 = new BigDecimal(tf_idfvalue1); 
        				BigDecimal tf_idf_data2 = new BigDecimal(tf_idfvalue2); 
        				return tf_idf_data2.compareTo(tf_idf_data1);
        			}  
        	});  

        	for (Entry<String, Double> entry: doctf_idf_keys)
        	{   
        		Text termDoc = new Text(entry.getKey()+"\t");
				Text tf_idfscore = new Text(entry.getValue().toString()+"\t");
				contex.write(termDoc,tf_idfscore); 

        	}   	
        }
    }

    public static class RankRetrievalReducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text commonword, Iterable<Text> wordvalues,Context contex)
        throws IOException, InterruptedException
        {
	        	Map<String,Double> wordmap = new HashMap<String,Double>();
	        	while(contex.nextKeyValue())
	        	{
	        		wordmap.put(contex.getCurrentKey().toString(), Double.parseDouble(contex.getCurrentValue().toString()));   
	        	}

	        	//Ranking of results through sorting based on tf-tdf scores
	            List<Map.Entry<String, Double>> tf_idf_docRecord = new ArrayList<Map.Entry<String, Double>>(wordmap.entrySet());
	        	Collections.sort(tf_idf_docRecord, new Comparator<Map.Entry<String, Double>>() 
	        	{  
	        		public int compare(Map.Entry<String, Double> entry1,Map.Entry<String, Double> entry2) 
        			{  
        				double tf_idfvalue1=Double.parseDouble(entry1.getValue().toString());
        				double tf_idfvalue2=Double.parseDouble(entry2.getValue().toString());
        				
        				BigDecimal tf_idf_data1 = new BigDecimal(tf_idfvalue1); 
        				BigDecimal tf_idf_data2 = new BigDecimal(tf_idfvalue2); 
        				return tf_idf_data2.compareTo(tf_idf_data1);
        			}  
	        	});  

	        	for (Entry<String, Double> entry: tf_idf_docRecord)
	        	{   
	        		Text termDoc = new Text(entry.getKey()+"\t");
					Text tf_idfscore = new Text(entry.getValue().toString()+"\t");
					contex.write(termDoc,tf_idfscore);  

	        	}   
        	}
        }

    

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
    {
    	Configuration conf = new Configuration();
    	
        Job job = new Job(conf,"RankRetrieval");
        job.setJarByClass(RankedIR.class);
        job.setMapperClass(RankedRetrievalMapping.class);
        job.setCombinerClass(RankRetrievalCmb.class);
        job.setReducerClass(RankRetrievalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        String inputquery = null;
        Stopwords stopword = new Stopwords();
        
        System.out.println(args[2]);
        inputquery=args[2];            //args[2] is the user query to retrieval system
	
        inputquery = inputquery.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
        
        inputquery = stopword.removeStemmedStopWords(inputquery);
			String[] query = inputquery.split(" ");
        	for(String word: query)
        	{ 
	        	if(userQuery.containsKey(word))
	        		userQuery.put(word, userQuery.get(word)+1);
				else
					userQuery.put(word, 1);
	        }
	
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
