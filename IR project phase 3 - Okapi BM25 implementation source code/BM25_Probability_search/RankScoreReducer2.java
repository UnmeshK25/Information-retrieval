import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;


  public  class RankScoreReducer2
         extends Reducer<Text,Text,Text,Text> 
  {

    private Text textValue = new Text();

    private HashMap<String,Integer>  relevantDocumentMap = Variables.relvantDocumentsMap;
    private HashMap<String,Integer>  stoppedStemmedWordMap = Variables.stopWordMap;

    private int numberOfDocumentss = Variables.numberOfDocs;
    private final double k_1 = 1.5;
    private final double b = 0.75;

    public void reduce(Text reducerKey, Iterable<Text> reducerValues,Context reducerContext) throws IOException, InterruptedException 
    {
  
      String[] documentIdMoldDocumentAvgLength = reducerKey.toString().split(",");
      String documentId = documentIdMoldDocumentAvgLength[0];
 
      Integer intValue1 = relevantDocumentMap.get(documentId);
      if (intValue1 != null) 
      {

        for (Text indValue : reducerValues) 
        {
        	String termfreqDocfreqStr = indValue.toString();
        	termfreqDocfreqStr = termfreqDocfreqStr.substring(1,termfreqDocfreqStr.length()-2);
        	String[] termTfreqDocfreqs = termfreqDocfreqStr.split(";");
          
        	double rsv_bm25wt = 0.0;
        	for(int i = 0; i < termTfreqDocfreqs.length; i++)
        	{

        		String[] termfreqDocfreqArray = termTfreqDocfreqs[i].split(",");
          
        		Integer intValue2 = stoppedStemmedWordMap.get(termfreqDocfreqArray[0]);
        		if (intValue2 != null) 
        		{     

        			//to compute BM25 weight
        			rsv_bm25wt += Math.log10( numberOfDocumentss / Integer.parseInt(termfreqDocfreqArray[2]) ) * (k_1 + 1) * Integer.parseInt(termfreqDocfreqArray[1]) / (k_1 * (1 - b) + b * (Double.parseDouble(documentIdMoldDocumentAvgLength[2]) 
                           / Double.parseDouble(documentIdMoldDocumentAvgLength[3]) ) +  Integer.parseInt(termfreqDocfreqArray[1]) );
        		}
        	}
        	reducerKey.set(documentId);
          textValue.set( String.valueOf(rsv_bm25wt));
          reducerContext.write(reducerKey, textValue);
        }
      }

  }
}
