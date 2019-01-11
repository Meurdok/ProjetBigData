import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile("src/main/resources/auth_500000.txt");
        /*JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile("out/authCount");
        */

        //Recuperer col 2 et 4

        //PART 1

        //Nettoyage des ligne contenant un "?"
        JavaRDD<String> clean = textFile.filter(line -> !line.contains("?"));

        //Comptage du nombre d'utilisation des machine par chaque utilisateur
        JavaPairRDD<String, Integer> taux = clean.map(s -> Arrays.asList(s.split(",")))
                                    .mapToPair(w -> new Tuple2<>("["+ w.get(1)+"-"+w.get(3)+"]",1))
                                    .reduceByKey((a,b) -> a + b);


        //Inversion de la RDD

        JavaPairRDD<Integer,String> reversed = taux.mapToPair(s -> s.swap());

        //Affichage sur la sortie standard des 10 accès les plus fréquents
        List<Tuple2<Integer, String>> top10 = reversed.sortByKey(false).take(10);
        top10.forEach(s -> System.out.println(s));

        //Output dans le dossier out/clean
        taux.saveAsTextFile("out/clean");
        System.out.println("DONE");
    }

}

