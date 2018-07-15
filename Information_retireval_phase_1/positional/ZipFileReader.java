package positional;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class ZipFileReader

    extends RecordReader<Text, BytesWritable>

{
	private Text textKey;
	private BytesWritable value;
	private boolean isDone = false;
    private FSDataInputStream fileInput;
    private ZipInputStream zipInput;

    @Override

    public void initialize( InputSplit splitInput, TaskAttemptContext taskContext )
        throws IOException, InterruptedException
    {
        FileSplit split = (FileSplit) splitInput;
        Configuration conf = taskContext.getConfiguration();
        Path filePath = split.getPath();

        FileSystem fs = filePath.getFileSystem( conf );

        fileInput = fs.open( filePath );
        zipInput = new ZipInputStream( fileInput );

    }

    @Override

    public boolean nextKeyValue()
        throws IOException, InterruptedException

    {

        ZipEntry zipEntry = null;
        try
        {
        	zipEntry = zipInput.getNextEntry();
        }
        catch ( ZipException exp )
        {
            if ( ZipFileInput.getLenient() == false )
                throw exp;
        }
        if ( zipEntry == null )
        {
        	isDone = true;
            return false;

        }
        textKey = new Text( zipEntry.getName() );
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();

        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            try
            {
                bytesRead = zipInput.read( temp, 0, 8192 );
            }
            catch ( EOFException e )
            {
                if ( ZipFileInput.getLenient() == false )
                    throw e;
                return false;
            }
            if ( bytesRead > 0 )
            	byteOutputStream.write( temp, 0, bytesRead );
            else
                break;
        }
        zipInput.closeEntry();     
        value = new BytesWritable( byteOutputStream.toByteArray() );
        return true;
    }

    @Override

    public float getProgress()
        throws IOException, InterruptedException
    {
        return isDone ? 1 : 0;
    }

    @Override

    public Text getCurrentKey()
        throws IOException, InterruptedException
    {
        return textKey;
    }

    @Override

    public BytesWritable getCurrentValue()
        throws IOException, InterruptedException
    {
        return value;
    }

    @Override

    public void close()
        throws IOException
    {

        try { zipInput.close(); } catch ( Exception ignore ) { }
        try { fileInput.close(); } catch ( Exception ignore ) { }
    }

}
