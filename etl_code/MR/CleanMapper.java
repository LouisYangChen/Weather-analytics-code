import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper
 extends Mapper<LongWritable, Text, Text, IntWritable> {
 private Text word = new Text();

 public void map(LongWritable key, Text value, Context context)
 throws IOException, InterruptedException {
 if(key.get() == 0) {
	 return;
 }
 String[] line = value.toString().split(",");
 for(String values:line) {
	 if(values == null) {
		 return;
	 }
 }
 int date_index = 1;
 int temp_index = 4;
 int humidity_index = 9;
 int precip_index = 10;
 int wind_index = 17;
 

 String output = String.format("%s\t%s\t%s\t%s\t%s", line[date_index],line[temp_index], line[humidity_index], line[precip_index], line[wind_index]);
 word.set(output);
 context.write(word,new IntWritable(1));


	 
 } 
 }