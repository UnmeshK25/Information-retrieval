import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;

public class RankScoreReducer3
       extends Reducer<NullWritable,Text,NullWritable,Text> 
{

  public void reduce(NullWritable reducerKey, Iterable<Text> reducerValues,Context reducerContext) throws IOException, InterruptedException 
  {

    TreeMap<Double, Text> topDocumentsTreeMap = new TreeMap<Double, Text>();

      for (Text txtValue : reducerValues) 
      {
          String[] documentIdScore = txtValue.toString().split("\\s+");
          topDocumentsTreeMap.put(Double.valueOf(documentIdScore[1]), new Text(txtValue));
          if (topDocumentsTreeMap.size() > 10) 
          {
        	  topDocumentsTreeMap.remove(topDocumentsTreeMap.firstKey());
          }
      }

      for (Text valueInd : topDocumentsTreeMap.values()) 
      {
    	  reducerContext.write(NullWritable.get(), valueInd);

          String[] documentsIdScoreStr = valueInd.toString().split("\\s+");
          Variables.rankedDocumentResultMap.put(documentsIdScoreStr[0], Double.valueOf(documentsIdScoreStr[1]));

      }
  }

}
