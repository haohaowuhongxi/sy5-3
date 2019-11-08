package sy533;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducer extends Reducer<Text, Sale, Text, Text> {
    protected void reduce(Text k3, Iterable<Sale> v3, Context context)
            throws IOException, InterruptedException{
        int totall = 0;
        float total2 = 0;
        for (Sale sale:v3){
            totall = totall + sale.getQuantity_sold();
            total2 = total2 + sale.getAmount_sold();
        }
        String total = "销售笔数:" + totall + "销售总额:" + total2;
        //输出
        context.write(k3, new  Text(total));
    }
}
