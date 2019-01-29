package main.bigdata;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;



public class Main {

    public static void main(String[] args){

        if(args.length == 1){
            if(args[0].equals("h")){
                System.out.println("Utilisation de l'application :");
                System.out.println("java -jar BigData.jar [file] [partie : 1|2|3|4] [write : 0=False|1=True] [frame length]");
                System.exit(0);
            }
        }

        if(args.length == 4){

            int part = Integer.parseInt(args[1]);
            int write = Integer.parseInt(args[2]);
            int frame = Integer.parseInt(args[3]);

            if(part <= 4 && part > 0 && write >= 0 && write <=1){
                //Create a SparkContext to initialize
                SparkConf conf = new SparkConf().setMaster("local").setAppName("BigData");

                // Create a Java version of the Spark Context
                JavaSparkContext sc = new JavaSparkContext(conf);

                Traitement traitement = new Traitement(sc, args[0]);

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
            }else{
                System.out.println("Utilisation de l'application :");
                System.out.println("java -jar BigData.jar [file] [partie : 1|2|3|4] [write : 0=False|1=True] [frame length]");
                System.exit(0);
            }
        }else{
            System.out.println("Utilisation de l'application :");
            System.out.println("java -jar BigData.jar [file] [partie : 1|2|3|4] [write : 0=False|1=True] [frame length]");
            System.exit(0);
        }


    }


}

