package sy533;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SaleMapper extends Mapper<LongWritable, Text, Text, Sale> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException{
        //数据
        String data = v1.toString();
        //分词
        String[] words = data.split(",");
        //    String t1 = StringUtils.substringAfter(data, ",");
        //    String t2 = StringUtils.substringAfter(t1, ",");
        //具体到时间的年份
        String[] year = words[2].split("-");
        //数据：148,4024,2001-10-27,3,999,1,23.17
        //创建销售对象
        Sale sale = new Sale();
        //设置属性
        //产品ID
        sale.setProd_id(Integer.parseInt(words[0]));
        //客户ID
        sale.setCust_id(Integer.parseInt(words[1]));
        //日期,具体到年份
        sale.setTime(year[0]);
        //渠道ID
        sale.setChannel_id(Integer.parseInt(words[3]));
        //促销ID
        sale.setPromo_id(Integer.parseInt(words[4]));
        //销售的数量
        sale.setQuantity_sold(Integer.parseInt(words[5]));
        //销售的总额
        sale.setAmount_sold(Float.parseFloat(words[6]));
        //输出
        context.write(new Text(sale.getTime()), sale);
    }
}
