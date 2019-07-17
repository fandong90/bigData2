import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExampleClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01");

        byte[] cf1 = Bytes.toBytes("cf");
        byte[] q1  = Bytes.toBytes("c");
        byte[] row3 = Bytes.toBytes("row3");
        byte[] row5 = Bytes.toBytes("row5");
        List<Get> gets = new ArrayList<Get>();
        Get get1 = new Get(row3);
        get1.addColumn(cf1,q1);
        gets.add(get1);

        Get get2 = new Get(row5);
        get2.addColumn(cf1,q1);
        gets.add(get2);



        HTable table = new HTable(conf,"test");

        Result[]  results = table.get(gets);

        for(Result result : results){
            String row  = Bytes.toString(result.getRow());
            System.out.println("row"+row);
            byte[] val =null;
            if(result.containsColumn(cf1,q1)){
                val =result.getValue(cf1,q1);
                System.out.println("row value :"+Bytes.toString(val));
            }
        }

        System.out.println("Second iteration...");

        for(Result result: results){
            for(KeyValue kv : result.raw()){
                System.out.println("Row "+Bytes.toString(kv.getRow())+" value: "+Bytes.toString(kv.getValue()));
            }
        }
    }
}
