package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class mrReducer extends Reducer<IntWritable, NullWritable, Text, NullWritable> {
    /*
    * 输入key即为源文件中的数字，value为空，因reduce会默认排序，因此只需要设置一个计数器即可获取当前序号
    * Reducer输出为：Text(xh+"\t"+key),null
    * */
    // 初始化序号作为计数器
    Integer xh = 0;
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        xh += 1; // 每行+1
        context.write(new Text(xh.toString() + "\t" + key.toString()), NullWritable.get());
    }
}
