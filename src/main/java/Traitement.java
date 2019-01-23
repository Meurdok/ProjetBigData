import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Traitement {

    JavaSparkContext sc;
    JavaRDD<String> textFile;

    public Traitement(JavaSparkContext jsc){
        sc = jsc;
        textFile = sc.textFile("src/main/resources/auth_500000.txt");
    }

    public void partie1(boolean write){
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

        if(write == true) {
            //Supprime le dossier de sortie si il existe deja
            deleteFile("out/Partie1");
            //Output dans le dossier out/Partie1
            taux.saveAsTextFile("out/Partie1");
        }

        System.out.println("--------------");
        System.out.println("Partie I finie");
        System.out.println("--------------");
    }

    public void partie2(boolean write){

        JavaRDD<String> clean = textFile.filter(line -> !line.contains("?"));

        //Solution en RDD, inutile pour la suite mais bon à savoir
      /*int i = 1;
        if(i == 0){
            JavaPairRDD<String, Integer> CoS2HRDD = clean.map(s -> Arrays.asList(s.split(",")))
                    .mapToPair(w -> new Tuple2<>("[" + w.get(1) + "|(" + w.get(4) + "," + w.get(5) + ")]", 1))
                    .reduceByKey((a, b) -> a + b);
        }*/

        JavaRDD<LogStruct> LogRDD = clean.map(s -> Arrays.asList(s.split(",")))
                .map(s -> new LogStruct(s.get(0),s.get(1),s.get(2),s.get(3),s.get(4),s.get(5),s.get(6),s.get(7),s.get(8)));

        SQLContext sqlC = new SQLContext(sc);

        //Création du dataframe pour la partie II

        DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);

        //QUESTION 1

        DataFrame q1a = this.transformationCount(df,"_2_userSource","_4_pcSource","_5_pcDest");

        q1a = q1a.withColumnRenamed("_2_userSource","utilisateurs");
        q1a = q1a.withColumnRenamed("_4_pcSource - _5_pcDest","connexion");

        q1a.show();

        if(write == true) {
            this.writeToFile(q1a,"Partie2/Utilisateurs_Connexions_Count");
        }


        DataFrame q1b = this.transformationList(q1a,"utilisateurs","connexion");

        if(write == true){
            this.writeToFile(q1b,"Partie2/UtilisateursEtConnexions");
        }

        //QUESTION 2

        DataFrame q2a = this.transformationCount(df,"_2_userSource","_8_OAuth","_9_Status");

        q2a = q2a.withColumnRenamed("_2_userSource","utilisateurs");
        q2a = q2a.withColumnRenamed("_8_OAuth - _9_Status","connexion");

        if(write==true){
            this.writeToFile(q2a,"Partie2/Utilisateur_Authentification_Count");
        }

        DataFrame q2b = this.transformationList(q2a,"utilisateurs","connexion");

        if(write == true) {
            this.writeToFile(q2b,"Partie2/Utilisateur_Authentification");
        }

        System.out.println("--------------");
        System.out.println("Partie II finie");
        System.out.println("--------------");

    }

    public void partie3(boolean write){

        JavaRDD<String> clean = textFile.filter(line -> !line.contains("?"));

        JavaRDD<LogStruct> LogRDD = clean.map(s -> Arrays.asList(s.split(",")))
                .map(s -> new LogStruct(s.get(0),s.get(1),s.get(2),s.get(3),s.get(4),s.get(5),s.get(6),s.get(7),s.get(8)));

        SQLContext sqlC = new SQLContext(sc);

        //Création du dataframe pour la partie II

        DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);

        for (String i: df.columns()) {
            if(!i.equals("_1_temps")){
                for (String j: df.columns()) {
                    if(j != i && !j.equals("_1_temps")){
                        for(String k: df.columns()){
                            if(k != i & k !=j && !k.equals("_1_temps")){
                                DataFrame ndfc = this.transformationCount(df,i,j,k);
                                if(write==true){this.writeSelectionToFile(ndfc,"Partie3/"+i+"-"+j+"-"+k+"-Count",10);}

                                /* DEMANDER SI IL FAUT CETTE FONCTIONNALITE
                                DataFrame ndfl = this.transformationList(ndfc,i,j+" - "+k);
                                if(write==true){this.writeSelectionToFile(ndfl,"Partie3/"+i+"-"+j+"-"+k+"-List",10);}
                                */
                            }
                        }
                    }
                }
            }
        }


    }

    public DataFrame transformationCount(DataFrame df,String col1,String col2, String col3){

        //Sélection des colonnes à traiter pour la question 1

        DataFrame CoS2Hdf = df.select(col1,col2,col3);

        //Transformation du dataframe pour la question 1-a)

        CoS2Hdf = CoS2Hdf.withColumn(col2+" - "+col3, concat(CoS2Hdf.col(col2),lit("-"),CoS2Hdf.col(col3)));
        CoS2Hdf = CoS2Hdf.drop(col2);
        CoS2Hdf = CoS2Hdf.drop(col3);
        CoS2Hdf = CoS2Hdf.groupBy(col1, col2+" - "+col3).count();

        return CoS2Hdf;
    }

    public DataFrame transformationList(DataFrame df, String col1, String col2){
        DataFrame ucdf = df.select(col1).distinct().unionAll(df.select(col2).distinct()).withColumnRenamed(col1,col1+" et "+col2);
        return ucdf;
    }

    public void writeToFile(DataFrame df,String filename){

        //Enregistrement au format json
        deleteFile("out/"+filename);
        df.coalesce(1).write().mode("append").json("out/"+filename);
    }

    public void writeSelectionToFile(DataFrame df,String filename, int rows){
        deleteFile("out/"+filename);
        DataFrame selection = df.limit(rows);
        selection.coalesce(1).write().mode("append").json("out/"+filename);
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
