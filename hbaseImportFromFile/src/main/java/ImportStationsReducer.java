import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ImportStationsReducer extends TableReducer<Text,Text,NullWritable> {

    private  final byte[] columnFamily = Bytes.toBytes("info");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Put put = new Put(key.getBytes());

        for(Text val : values){
            String[] valArr = val.toString().split("\t");
            int valLength = valArr.length;
            byte[] qualify=null;
            for (int i=0;i<valLength;i++){
                if(i==0) {
                    qualify = Bytes.toBytes("station");
                }
                else if(i ==1){
                    qualify =Bytes.toBytes("ctry");
                }
                else if(i==2){
                    qualify =Bytes.toBytes("st");
                }
                else if(i==3){
                    qualify =Bytes.toBytes("call");
                }
                else if(i==4){
                    qualify = Bytes.toBytes("lat");
                }
                else if(i==5){
                    qualify = Bytes.toBytes("lon");
                }
                else if(i==6){
                    qualify = Bytes.toBytes("elev");
                }else{
                    qualify = Bytes.toBytes("other");
                }

                put.add(columnFamily,qualify,Bytes.toBytes(valArr[i]));

            }

            context.write(NullWritable.get(),put);
        }
    }
}
