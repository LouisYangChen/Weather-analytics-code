import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

public class CleanReducer
extends Reducer<Text,IntWritable,Text,NullWritable> {
private IntWritable result = new IntWritable();
@Override
public void reduce(Text key, Iterable<IntWritable> values,
               Context context
) throws IOException, InterruptedException {
context.write(new Text(key),NullWritable.get());
}
}