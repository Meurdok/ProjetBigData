import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.reflect.io.Directory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class Main {

    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BigData");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile("src/main/resources/auth_500000.txt");

        //PARTIE I

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

        //Supprime le dossier de sortie si il existe deja
        File file = new File("out/clean");
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Output dans le dossier out/clean
        taux.saveAsTextFile("out/clean");
        System.out.println("--------------");
        System.out.println("Partie I finie");
        System.out.println("--------------");

        //Partie II

        String schemaString = "name age";

        //JavaRDD<String> fields = schemaString.split(" ").map(fieldName -> new StructField(fieldName, StringType, true, null));

        //val schema = StructType(fields);
        //5. Convert records of the RDD (people) to Rows

        //val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
        //6. Apply the schema to the RDD

        //val peopleDF = spark.createDataFrame(rowRDD, schema)
        //6. Creates a temporary view using the DataFrame

        //peopleDF.createOrReplaceTempView("people")
        //7. SQL can be run over a temporary view created using DataFrames

        //val results = spark.sql("SELECT name FROM people")
        //8.The results of SQL queries are DataFrames and support all the normal RDD operations. The columns of a row in the result can be accessed by field index or by field name

        //results.map(attributes => "Name: " + attributes(0)).show()
        //This will produce an output similar to the following:

        /*...
        +-------------+
        |        value|
        +-------------+
        |Name: Michael|
        |Name: Andy|
        |Name: Justin|
        +-------------+
        */
    }

}

