import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

/**
 * @author alexander.solovyov
 * @since 27.12.16
 */
public class Main {
    public static void main(final String[] args) {

        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/META-INF/spring-config.xml");

        final String logFile = System.getProperty("user.dir") + File.separator + "java-spark-sandbox.iml";

        final SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> logData = sc.textFile(logFile).cache();

        final long numAs = logData.filter(s -> s.contains("a")).count();

        final long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
