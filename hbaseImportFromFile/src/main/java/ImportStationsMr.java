import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class ImportStationsMr {

    private  static  final  String hbaseTable="stations";
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop01");
        configuration.set(TableOutputFormat.OUTPUT_TABLE,hbaseTable);
        Job job = Job.getInstance(configuration,ImportStationsMr.class.getSimpleName());
        TableMapReduceUtil.addDependencyJars(job);
        job.setMapperClass(StationsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ImportStationsReducer.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        job.setOutputFormatClass(TableOutputFormat.class);
        job.waitForCompletion(true);
    }
}
