import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationsMapper extends Mapper<LongWritable, Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lineArr = value.toString().split("\t");
        if(lineArr.length>=2){
            outKey.set(lineArr[0]+lineArr[1]);
            String lineValue="";
            for(int i=2;i<lineArr.length;i++){
                lineValue +=lineArr[i]+"\t";
            }

            outValue.set(lineValue.substring(0,lineValue.length()-1));

        }else{

            outKey.set(value);

        }
        context.write(outKey,outValue);

    }
}
