package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mrMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取文件，key即为每行的数据，value为null
        Integer number = new Integer(value.toString());
        context.write(new IntWritable(number), NullWritable.get());
    }
}
