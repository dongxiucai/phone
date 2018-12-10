package com.run.test;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import javax.swing.border.EtchedBorder;
        import java.io.IOException;
        import java.net.URL;
        import java.util.regex.Matcher;
        import java.util.regex.Pattern;

public class ETLTest {
    public static void main(String[] args) throws Exception{
        // 配置信息
        Configuration configuration = new Configuration();
        // 声明job
        Job job = Job.getInstance(configuration);
        // 设置函数入口
        job.setJarByClass(ETLTest.class);
        // 设置mapper和reducer的类
        job.setMapperClass(ETLMapper.class);
        // 设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Demo\\hadoop\\input\\etl.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\Demo\\hadoop\\ouput\\etl7"));
        // 提交
        boolean re = job.waitForCompletion(true);
        System.exit(re ? 0 : 1);
    }
}

/**
 * mapper端
 */
class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    String data = "";
    String xq = "";
    String tp = "";
    String pj = "";
    String xin = "";
    int status = 0;
    Text ke = new Text();
    /**
     * 清洗日志
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 首先判断，一行是否存在 get page:
        if (line.contains("get page: ")) {
            // 切割文件
            String page = line.split(": ")[1];
            if(xq.equals("")){
                xq = "-";
            }
            // 赋值，清理字符串
            String contextDate = "^"+tp+"\t"+pj+"\t"+xin+"\t"+xq;
            // 初始化
            tp = "";
            pj = "";
            xin = "";
            xq = "";
            status = 0;
            String contextDetalisImg = ETLUtils.detailsImg;
            if(contextDetalisImg.equals("")){
                contextDetalisImg = "-";
            }
            ETLUtils.initImg();
            // 提交信息到文件
            ke.set(contextDate.replaceAll(" ","") + "\t" + contextDetalisImg);
            // 酒店房间图片中间，隔开 \t酒店房间评价\t酒店房间信息\t酒店房间详情\t酒店房间详情图片中间用|隔开
            context.write(ke,NullWritable.get());
        } else {
            // 继续累加信息累加
            // 获取文件中的一行,切分
            String[] spl = line.split("\\[");
            // 取出类型
            String type = spl[0];
            if(type.equals("酒店房间信息")){
                status = 3;
            }
            // 判断类型
            // 如果是酒店房间图片
            if (type.equals("酒店房间图片")) {
                String imgUrls = ETLUtils.imgUrls(spl[1]);
                //data = imgUrls + "\t" + data;
                tp = imgUrls;
                // 状态加1
                status = 1;
            } else if (type.equals("酒店房间平价")) {
                String evaluate = "";
                evaluate = ETLUtils.evaluate(spl[1]);
                status = 2;
                // data = data + "\t" + evaluate;
                pj = pj + evaluate;
            }else if(status == 2){
                String evaluate = "";
                evaluate = ETLUtils.evaluate(line);
                pj = pj + evaluate;
            } else if (type.equals("酒店房间信息")) {
                String information = ETLUtils.information(spl[1]);
                // data = data + "\t" + information;
                xin = information;
            } else if(type.equals("酒店房间详情")){
                status = 4;
            }else if(status == 4){
                String details = ETLUtils.details(line);
                xq = xq + details;
            }
        }
    }
}

