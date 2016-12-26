import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * @author alexander.solovyov
 * @since 27.12.16
 */
public class Main {
    public static void main(String[] args) {
        String logFile = System.getProperty("user.dir") + File.separator + "java-spark-sandbox.iml";

        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();

        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
