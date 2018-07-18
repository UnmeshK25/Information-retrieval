package uniword;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ZipFileInput
    extends FileInputFormat<Text, BytesWritable>
{
    private static boolean lenientFlag = false;
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }
    @Override

    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
        throws InterruptedException
    {
        return new ZipFileReader();
    }
    public static void setLenient( boolean lenient )
    {
    	lenientFlag = lenient;
    }
    public static boolean getLenient()
    {
        return lenientFlag;
    }
}