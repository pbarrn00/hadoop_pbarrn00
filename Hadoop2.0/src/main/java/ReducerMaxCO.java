import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerMaxCO extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	//He quitado el decimal format
	/**
	 *@param Text key, Iterable<DoubleWritable> values, Context context
	 *
	 *@return void
	 *@exception  IOException, InterruptedException 
	 *
	 */
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		int n = 0;
		double totalCO = 0.0;

		for (DoubleWritable value : values) {
			totalCO += value.get();
			n++;
		}
		
		context.write(key,new DoubleWritable(totalCO/n));
	}
  
}

