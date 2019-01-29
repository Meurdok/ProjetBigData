package main.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;

public class Traitement {

    JavaSparkContext sc;
    JavaRDD<String> textFile;

    public Traitement(JavaSparkContext jsc,String path){
        sc = jsc;
        textFile = sc.textFile(path);
    }

    public void partie1(int write){

        System.out.println("--------------");
        System.out.println("Début Partie I");
        System.out.println("--------------");

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
        System.out.println("-----Top 10 des accès-----");
        top10.forEach(s -> System.out.println(s));

        if(write == 1) {
            //Supprime le dossier de sortie si il existe deja
            deleteFile("out/Partie1");
            //Output dans le dossier out/Partie1
            taux.saveAsTextFile("out/Partie1");
        }

        System.out.println("--------------");
        System.out.println("Partie I finie");
        System.out.println("--------------");
    }

    public void partie2(int write){

        System.out.println("--------------");
        System.out.println("Début Partie II");
        System.out.println("--------------");

        if(write==1){
            deleteFile("/out/Partie2");
        }

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

        //On renomme les colonnes pour respecter le format de la question
        q1a = q1a.withColumnRenamed("_2_userSource","utilisateurs");
        q1a = q1a.withColumnRenamed("_4_pcSource - _5_pcDest","connexion");

        q1a.show();

        if(write == 1) {
            this.writeToFile(q1a,"Partie2/Utilisateurs_Connexions_Count");
        }


        DataFrame q1b = this.transformationList(q1a,"utilisateurs","connexion");

        if(write == 1){
            this.writeToFile(q1b,"Partie2/UtilisateursEtConnexions");
        }

        //QUESTION 2

        DataFrame q2a = this.transformationCount(df,"_2_userSource","_8_OAuth","_9_Status");

        //On renomme les colonnes pour respecter le format de la question
        q2a = q2a.withColumnRenamed("_2_userSource","utilisateurs");
        q2a = q2a.withColumnRenamed("_8_OAuth - _9_Status","connexion");

        if(write==1){
            this.writeToFile(q2a,"Partie2/Utilisateur_Authentification_Count");
        }

        DataFrame q2b = this.transformationList(q2a,"utilisateurs","connexion");

        if(write == 1) {
            this.writeToFile(q2b,"Partie2/Utilisateur_Authentification");
        }

        System.out.println("--------------");
        System.out.println("Partie II finie");
        System.out.println("--------------");

    }

    public void partie3(int write){

        System.out.println("--------------");
        System.out.println("Début Partie III");
        System.out.println("--------------");

        if(write==1){
            deleteFile("/out/Partie3");
        }


        JavaRDD<String> clean = textFile.filter(line -> !line.contains("?"));

        JavaRDD<LogStruct> LogRDD = clean.map(s -> Arrays.asList(s.split(",")))
                .map(s -> new LogStruct(s.get(0),s.get(1),s.get(2),s.get(3),s.get(4),s.get(5),s.get(6),s.get(7),s.get(8)));

        SQLContext sqlC = new SQLContext(sc);

        //Création du dataframe pour la partie III

        DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);

        for (String i: df.columns()) { //Colonne 1 i
            if(!i.equals("_1_temps")){
                for (String j: df.columns()) { //Colonne 2 j
                    if(!j.equals(i) && !j.equals("_1_temps")){
                        for(String k: df.columns()){ //Colonne 3 k
                            if(!k.equals(i) && !k.equals(j) && !k.equals("_1_temps") && IsC1SupC2(k,j)){
                                DataFrame ndfc = this.transformationCount(df,i,j,k);
                                if(write==1){this.writeSelectionToFile(ndfc,"Partie3/"+i+"-"+j+"-"+k+"-Count",10);}


                                DataFrame ndfl = this.transformationList(ndfc,i,j+" - "+k);
                                if(write==1){this.writeSelectionToFile(ndfl,"Partie3/"+i+"-"+j+"-"+k+"-List",10);}

                            }
                        }
                    }
                }
            }
        }
        System.out.println("--------------");
        System.out.println("Partie III finie");
        System.out.println("--------------");

    }

    public void partie4(int write, int ft) {

        System.out.println("--------------");
        System.out.println("Début Partie VI");
        System.out.println("--------------");

        if(write==1){
            deleteFile("/out/Partie4");
        }

        JavaRDD<String> clean = textFile.filter(line -> !line.contains("?"));

        JavaRDD<LogStruct> LogRDD = clean.map(s -> Arrays.asList(s.split(",")))
                .map(s -> new LogStruct(s.get(0),s.get(1),s.get(2),s.get(3),s.get(4),s.get(5),s.get(6),s.get(7),s.get(8)));

        SQLContext sqlC = new SQLContext(sc);

        //Création du dataframe pour la partie IV

        DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);

        //On récupère la valeur maximale de temps

        DataFrame max = df.agg(max("_1_temps"));
        int m = Integer.parseInt(max.head().getString(0));

        //Pour chaque fenetre on récupère un dataframe de la taille de cette fenetre

        for(int bound = 0; bound+ft < m; bound+=ft) {
            DataFrame frame = df.where("_1_temps > "+bound+" and _1_temps < "+ (bound+ft));
            for (String i : frame.columns()) {
                if (!i.equals("_1_temps")) {
                    for (String j : frame.columns()) {
                        if (j != i && !j.equals("_1_temps")) {
                            for (String k : frame.columns()) {
                                if (k != i & k != j && !k.equals("_1_temps") && IsC1SupC2(k,j)) {
                                    DataFrame ndfc = this.transformationCount(frame, i, j, k);
                                    int sup = bound + ft;
                                    //ndfc = ndfc.join(frame.select("_1_temps",i),i);

                                    if(write==1){this.writeSelectionToFile(ndfc,"Partie4/"+i+"-"+j+"-"+k+"-Count"+"-"+bound+"to"+sup,10);}

                                    DataFrame ndfl = this.transformationList(ndfc,i,j+" - "+k);
                                    if(write==1){this.writeSelectionToFile(ndfl,"Partie4/"+i+"-"+j+"-"+k+"-List"+"-"+bound+"to"+sup,10);}

                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("--------------");
        System.out.println("Partie IV finie");
        System.out.println("--------------");
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

    public static boolean IsC1SupC2(String col1, String col2){

        String[] comp1 = col1.split("_");
        String[] comp2 = col2.split("_");

        System.out.println("Indice col1 : "+comp1[1]+" - Indice col2 : "+comp2[1]);

        return Integer.parseInt(comp1[1]) > Integer.parseInt(comp2[1]);

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
