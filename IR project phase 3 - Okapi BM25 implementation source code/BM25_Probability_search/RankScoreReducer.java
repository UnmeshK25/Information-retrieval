import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;

  public  class RankScoreReducer
         extends Reducer<Text,Text,Text,Text> 
  {

    private Text txtValue1 = new Text();
    private HashMap<String,Integer>  stoppedWordMap = Variables.stopWordMap;

    public void reduce(Text reducerKey, Iterable<Text> reducerValues,Context reducerContext) throws IOException, InterruptedException 
    {
     
      String[] termDocumentfreq = reducerKey.toString().split(",");
      String termStr = termDocumentfreq[0];
      Integer valueInt = stoppedWordMap.get(termStr);
      
      if (valueInt != null) 
      {

        Integer valueInt1 = Variables.queryTermDocumentfreqMap.get(termStr);
        if(valueInt1 == null)
        {
        	Variables.queryTermDocumentfreqMap.put(termDocumentfreq[0], Integer.valueOf(termDocumentfreq[1]));
        }

        for (Text valueInd : reducerValues) 
        {
        	String documents = valueInd.toString();
        	documents = documents.substring(1,documents.length()-2);
        	String[] documentssArray= documents.split(";");
          
          for(int i = 0; i < documentssArray.length; i++)
          {
           
            String documentId = documentssArray[i];
            Integer valueInt2 = Variables.relvantDocumentsMap.get(documentId);
          
            if(valueInt2 == null)
            {
             
              Variables.relvantDocumentsMap.put(documentId,1);
            }

            reducerKey.set(documentId);
            txtValue1.set(termStr);
            reducerContext.write(reducerKey, txtValue1);

          }

        }

      }
    }
  
  }