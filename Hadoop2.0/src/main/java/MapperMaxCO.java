import java.io.IOException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperMaxCO extends Mapper<Object, Text, Text, DoubleWritable> {

	/** He cambiado esta disposición
	 * DISPOSICIÓN DEL CSV DE DATOS
	 * Fecha; CO (mg/m3); NO (ug/m3); NO2 (ug/m3); O3 (ug/m3); PM10 (ug/m3); PM25 (ug/m3); SO2 (ug/m3); PROVINCIA; ESTACIÓN
	 * 
	 * DISPOSICIÓN DEL REDUCE
	 * PROVINCIA; CO(mg/m3);
	 */
	/**
	 *
	 *@param Object key, Text value, Context context
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(";");
		
		//He cogido solo los valores del monoxido de carbono(CO) y la provincia
		//He cambiado format por trim
		final String co = values[1].trim();
		final String province = values[8].trim();
		
		//He usado is creatable, isNumber deprecated
		if (NumberUtils.isCreatable(co.toString())) {
			context.write(new Text(province), new DoubleWritable(NumberUtils.toDouble(co)));
		}
	}
}
