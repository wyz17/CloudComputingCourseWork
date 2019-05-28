
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRFileCount extends Configured implements Tool {

    public static class FirstMapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            long number = Long.valueOf(data[0]); //get data
            String path = ((FileSplit) context.getInputSplit()).getPath().toString(); //get path
            int index = path.lastIndexOf("/"); //get the position of last "/" and split it into filename
            String fileName = path.substring(index + 1); //get filename
            context.write(new Text(fileName), new LongWritable(number));
        }
    }

    public static class FirstReduce extends Reducer<Text, LongWritable, Text, Text> {
        float digit = 0; //count digit
        float ten = 0; //count ten
        float hundred = 0; //count hundreds
        float thousand = 0;
        float ten_thousand = 0;
        float hundred_thousand = 0;
        float sum = 0;
        DecimalFormat df = new DecimalFormat("0.000");

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable val : values) {
                if (Integer.valueOf(val.toString()) < 10)
                    digit++;
                if (Integer.valueOf(val.toString()) < 100 && Integer.valueOf(val.toString()) >= 10)
                    ten++;
                if (Integer.valueOf(val.toString()) < 1000 && Integer.valueOf(val.toString()) >= 100)
                    hundred++;
                if (Integer.valueOf(val.toString()) < 10000 && Integer.valueOf(val.toString()) >= 1000)
                    thousand++;
                if (Integer.valueOf(val.toString()) < 100000 && Integer.valueOf(val.toString()) >= 10000)
                    ten_thousand++;
                if (Integer.valueOf(val.toString()) < 1000000 && Integer.valueOf(val.toString()) >= 100000)
                    hundred_thousand++;
            }
            sum = digit + ten + hundred + thousand + ten_thousand + hundred_thousand;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //get percentage of each unit
            context.write(new Text( "个位数"),new Text(df.format(digit/sum * 100)+ "%"));
            context.write(new Text( "十位数"),new Text(df.format(ten/sum * 100) + "%"));
            context.write(new Text("百位数"),new Text(df.format(hundred/sum * 100) + "%"));
            context.write(new Text("千位数"),new Text(df.format(thousand/sum * 100) + "%"));
            context.write(new Text("万位数"),new Text(df.format(ten_thousand/sum * 100) + "%"));
            context.write(new Text("十万位数"),new Text(df.format(hundred_thousand/sum * 100) + "%"));
        }
    }

    public static class SecondMapClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            double number = Double.valueOf(data[1].substring(0, 5));
            context.write(new Text(data[0]), new DoubleWritable(number)); //get percentage and convert them into double
        }
    }

    public static class SecondReduce extends Reducer<Text, DoubleWritable, Text, Text> {
        int n = 20;
        //use TreeSet to store MyValue(file info and number)
        TreeSet<MyValue> digit = new TreeSet<>();
        TreeSet<MyValue> ten = new TreeSet<>();
        TreeSet<MyValue> hundred = new TreeSet<>();
        TreeSet<MyValue> thousand = new TreeSet<>();
        TreeSet<MyValue> ten_thousand = new TreeSet<MyValue>();
        TreeSet<MyValue> hundred_thousand = new TreeSet<MyValue>();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                MyValue v = new MyValue();
                String temp = key.toString(); //use to compare
                String dig = temp.substring(temp.length() - 4).trim();//acquire units
                if (dig.equals("个位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    digit.add(v);
                    if (digit.size() > n) {
                        digit.pollFirst(); //delete minimum number
                    }
                } else if (dig.equals("十位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    ten.add(v);
                    if (ten.size() > n) {
                        ten.pollFirst();
                    }
                } else if (dig.equals("百位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    ten.add(v);
                    hundred.add(v);
                    if (hundred.size() > n) {
                        hundred.pollFirst();
                    }
                } else if (dig.equals("千位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    thousand.add(v);
                    if (thousand.size() > n) {
                        thousand.pollFirst();
                    }
                } else if (dig.equals("万位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    ten_thousand.add(v);
                    if (ten_thousand.size() > n) {
                        ten_thousand.pollFirst();
                    }
                } else if (dig.equals("十万位数")) {
                    v.setInfo(key.toString());
                    v.setNum(val.get());
                    hundred_thousand.add(v);
                    if (hundred_thousand.size() > n) {
                        hundred_thousand.pollFirst();
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            while(!digit.isEmpty()){ //traverse all the value in TreeSet
                String info = digit.pollLast().getInfo();
                double num = (digit.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
            while(!ten.isEmpty()){
                String info = ten.pollLast().getInfo();
                double num = (ten.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
            while(!hundred.isEmpty()){
                String info = hundred.pollLast().getInfo();
                double num = (hundred.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
            while(!thousand.isEmpty()){
                String info = thousand.pollLast().getInfo();
                double num = (thousand.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
            while(!ten_thousand.isEmpty()){
                String info = ten_thousand.pollLast().getInfo();
                double num = (ten_thousand.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
            while(!hundred_thousand.isEmpty()){
                String info = hundred_thousand.pollLast().getInfo();
                double num = (hundred_thousand.pollLast()).getNum();
                context.write(new Text(info), new Text(num +"%"));
            }
        }
    }



    public static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            int num = Integer.valueOf((key.toString()).substring(4));
            return num % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("key.value.separator.in.input.line", ",");

        @SuppressWarnings("deprecation")
        Job job1 = new Job(conf, "MyJob");
        job1.setJarByClass(MRFileCount.class);


        Path in = new Path("data/");
        Path out = new Path("data/output");
        File output = new File("data/output");
        if (output.isDirectory() && output.exists()) {
            deleteFile(output);
        }
        FileInputFormat.setInputPaths(job1, in);
        FileOutputFormat.setOutputPath(job1, out);

        job1.setMapperClass(FirstMapClass.class);
//        job1.setPartitionerClass(MyPartitioner.class);
//        job1.setNumReduceTasks(100);
        job1.setReducerClass(FirstReduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        Job job2 = new Job(conf, "Job2");
        job2.setJarByClass(MRFileCount.class);


        Path in2 = new Path("data/output");
        Path out2 = new Path("data/output/file");
        File output2 = new File("data/output/file");
        if (output2.isDirectory() && output2.exists()) {
            deleteFile(output2);
        }
        FileInputFormat.setInputPaths(job2, in2);
        FileOutputFormat.setOutputPath(job2, out2);

        job2.setMapperClass(SecondMapClass.class);
        job2.setReducerClass(SecondReduce.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MRFileCount(), args);
        System.exit(res);
    }

    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }

}

class MyValue implements Comparable<MyValue>{
    String info;
    double num;

    public MyValue() {
    }

    public MyValue(String info, double num) {
        this.info = info;
        this.num = num;
    }

    public String getInfo() {
        return info;
    }

    public double getNum() {
        return num;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public void setNum(double num) {
        this.num = num;
    }


    @Override
    public int compareTo(MyValue o1) {
        if(o1.getNum()==this.getNum() && o1.getInfo().equals(this.getInfo()))
            return 0;
        if(o1.getNum() > this.getNum())
            return -1;
        else
            return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MyValue)) return false;
        MyValue myValue = (MyValue) o;
        return Double.compare(myValue.getNum(), getNum()) == 0 &&
                Objects.equals(getInfo(), myValue.getInfo());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInfo(), getNum());
    }
}