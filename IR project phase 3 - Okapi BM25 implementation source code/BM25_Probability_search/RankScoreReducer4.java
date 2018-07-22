import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;


  public class RankScoreReducer4
         extends Reducer<Text,Text,Text,Text> 
  {

	  private Text resultTxt = new Text();
	  public static HashMap<String,Double>  toprankResultMap = Variables.rankedDocumentResultMap;
	  public void reduce(Text reducerKey, Iterable<Text> reducerValues,Context reducerContext) throws IOException, InterruptedException 
	  {
		  String documentID = reducerKey.toString();   
		  Double rankObject1 = toprankResultMap.get(documentID);

		  if (rankObject1 != null) 
		  {
			  for (Text valInd : reducerValues) 
			  {
				  reducerKey.set(documentID);
				  resultTxt.set(valInd);
				  reducerContext.write(reducerKey, resultTxt);
			  }

		  }
  }
}
