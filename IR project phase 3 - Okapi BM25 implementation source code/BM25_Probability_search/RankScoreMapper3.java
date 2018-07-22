import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import java.util.TreeMap;

public  class RankScoreMapper3
  	extends Mapper<Object, Text, NullWritable, Text> 
{
  	private TreeMap<Double, Text> topRankedDocumentssTreeMap = new TreeMap<Double, Text>();
  	public void map( Object rankKey, Text rankValue, Context rankContext)
      throws IOException, InterruptedException 
  	{
      	String[] documentIdScore = rankValue.toString().split("\\s+");
      	topRankedDocumentssTreeMap.put(Double.valueOf(documentIdScore[1]), new Text(rankValue)); 
        if (topRankedDocumentssTreeMap.size() > 20)   //For top 20 results
        {
        	topRankedDocumentssTreeMap.remove(topRankedDocumentssTreeMap.firstKey());
        }  
   	}
   	protected void cleanup(Context rankcontext) 
   	throws IOException, InterruptedException 
   	{
        for ( Text txtValue : topRankedDocumentssTreeMap.values() ) 
        {
        	rankcontext.write(NullWritable.get(), txtValue);
        }

    }

}
