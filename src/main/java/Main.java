import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;



public class Main {

    public static void main(String[] args){

        int part = Integer.parseInt(args[0]);
        boolean write = Boolean.parseBoolean(args[1]);
        int frame = Integer.parseInt(args[2]);

        if(args.length != 3 || part > 4 || part <= 0){
            System.out.println("Utilisation de l'application :");
            System.out.println("java -jar BigData.jar [partie : 1|2|3|4] [write : true|false] [frame length]");
            System.exit(0);
        }


        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BigData");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        Traitement traitement = new Traitement(sc);

        switch(part) {
            case 1:
                //Partie I
                traitement.partie1(write); break;

            case 2:
                //Partie II
                traitement.partie2(write); break;

            case 3:
                //Partie III
                traitement.partie3(write); break;

            case 4:
                //Partie IV
                traitement.partie4(write, frame); break;
        }
    }

}

