import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import org.apache.spark.sql.SQLContext;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;


public class Main {

    public static void main(String[] args){

        boolean write = true;

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
        top10.forEach(s -> System.out.println(s)); // => ADD OUTPUT VERS JSON

        if(write == true) {
            //Supprime le dossier de sortie si il existe deja
            deleteFile("out/Partie1");
            //Output dans le dossier out/Partie1
            taux.saveAsTextFile("out/Partie1");
        }

        System.out.println("--------------");
        System.out.println("Partie I finie");
        System.out.println("--------------");

        //Partie II

        //Solution en RDD, inutile pour la suite mais bon à savoir
        int i = 1;
        if(i == 0){
            JavaPairRDD<String, Integer> CoS2HRDD = clean.map(s -> Arrays.asList(s.split(",")))
                    .mapToPair(w -> new Tuple2<>("[" + w.get(1) + "|(" + w.get(4) + "," + w.get(5) + ")]", 1))
                    .reduceByKey((a, b) -> a + b);
        }

        JavaRDD<LogStruct> LogRDD = clean.map(s -> Arrays.asList(s.split(",")))
                                    .map(s -> new LogStruct(s.get(0),s.get(1),s.get(2),s.get(3),s.get(4),s.get(5),s.get(6),s.get(7),s.get(8)));

        SQLContext sqlC = new SQLContext(sc);

        //Création du dataframe pour la partie II

        DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);

        //Sélection des colonnes à traiter pour la question 1

        DataFrame CoS2Hdf = df.select("_2_userSource","_4_pcSource","_5_pcDest");

        //Transformation du dataframe pour la question 1-a)

        CoS2Hdf = CoS2Hdf.withColumn("connexion", concat(CoS2Hdf.col("_4_pcSource"),lit("-"),CoS2Hdf.col("_5_pcDest")));
        CoS2Hdf = CoS2Hdf.drop("_4_pcSource");
        CoS2Hdf = CoS2Hdf.drop("_5_pcDest");
        CoS2Hdf = CoS2Hdf.withColumnRenamed("_2_userSource","utilisateurs");
        CoS2Hdf = CoS2Hdf.groupBy("utilisateurs", "connexion").count();

        if(write == true) {
            //Enregistrement au format json
            deleteFile("out/Partie2/Utilisateurs_Connexions_Count");
            CoS2Hdf.coalesce(1).write().mode("append").json("out/Partie2/Utilisateurs_Connexions_Count");
        }
        //Transformation pour la question 1-b)

        DataFrame ucdf = CoS2Hdf.select("utilisateurs").distinct().unionAll(CoS2Hdf.select("connexion").distinct()).withColumnRenamed("utilisateurs","utilisateurs et connexions");
        if(write==true) {
            //Enregistrement au format json
            deleteFile("out/Partie2/UtilisateursEtConnexions");
            ucdf.coalesce(1).write().mode("append").json("out/Partie2/UtilisateursEtConnexions");
        }

        //Sélection des colonnes pour la question 2

        DataFrame userAuth = df.select("_2_userSource","_8_OAuth","_9_Status");

        //Transformation pour la question 2-a)

        userAuth = userAuth.withColumn("connexions", concat(userAuth.col("_8_OAuth"),lit(" , "),userAuth.col("_9_Status")));
        userAuth = userAuth.drop("_8_OAuth").drop("_9_Status").withColumnRenamed("_2_userSource","utilisateurs");
        userAuth = userAuth.groupBy("utilisateurs","connexions").count();
        userAuth = userAuth.withColumnRenamed("count","poids");

        if(write == true) {
            //Enregistrement au format json
            deleteFile("out/Partie2/Utilisateur_Authentification_Count");
            userAuth.coalesce(1).write().mode("append").json("out/Partie2/Utilisateur_Authentification_Count");
        }

        //Transformation pour la question 2-b)
        DataFrame uadf = userAuth.select("utilisateurs").distinct().unionAll(userAuth.select("connexions").distinct()).withColumnRenamed("utilisateurs","utilisateurs et connexions");

        if(write == true) {
            //Enregistrement au format json
            deleteFile("out/Partie2/Utilisateur_Authentification");
            uadf.coalesce(1).write().mode("append").json("out/Partie2/Utilisateur_Authentification");
        }

        System.out.println("--------------");
        System.out.println("Partie II finie");
        System.out.println("--------------");


    }

    public static void deleteFile(String path){
        File file = new File(path);
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

