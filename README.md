# ProjetBigData

Projet réalisé par Paul Decanter, Doriane Lecam et Samy Ben Tounes
 
Le code peut être trouvé sur github : https://github.com/Meurdok/ProjetBigData

Chaque partie du projet possède une fonction dédiée dans la classe Traitement.java


APIs :

            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>1.6.2</version>

            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>1.6.2</version>

##Utilisation de l'application

Pour lancer l'application :

    java -jar BigData.jar [<u>Partie</u> : 1|2|3|4] [<u>Write</u> : true | false] [<u>Frame</u> : {1,100}]

Partie : Lance le code de la partie choisie  
Write : Ecrit les fichiers au format json dans le fichier <i>out/</i>   
Frame : Pour la Partie 4, défini la taille de la fenêtre temporelle  
##Partie I

Le RDD peut être trouvé dans le dossier <u>out/Partie1</u>

```
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
```

Les dix accès les plus fréquents:

(3837,[C599$@DOM1-C1619])  
(3384,[C585$@DOM1-C585])  
(2929,[C1114$@DOM1-C1115])  
(2925,[C743$@DOM1-C743])  
(2725,[C104$@DOM1-C105])  
(2460,[C567$@DOM1-C574])  
(2345,[C123$@DOM1-C527])  
(2002,[C1617$@DOM1-C1618])  
(1930,[C538$@DOM1-C539])  
(1907,[U22@DOM1-C506])

##Partie II

Les dataframes sont enregistrés dans le dossier <u>out/Partie2</u>

La question 1 correspond aux fichiers <u>Utilisateurs_Connexions_Count</u> et <u>UtilisateursEtConnexions</u>  

La question 2 correspond aux fichiers <u>Utilisateur_Authentification_Count</u> et <u>Utilisateur_Authentification</u> 
 
Pour créer le dataframe, on crée une classe <b>LogStruct.java</b> afin d'avoir une structure de donnée.

```
 DataFrame df = sqlC.createDataFrame(LogRDD,LogStruct.class);
```

Pour répondre aux questions 1 et 2, on utilise les fonctions suivantes :

- Pour les questions 1.(a) et 2.(a), on utilise la fonction suivante sur le datagrame généré précédemment: :

```
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
```
- Pour les questions 1.(b) et 2.(b), on récupère le dataframe généré par la fonction précédente et on applique la fonction suivante:

```
    public DataFrame transformationList(DataFrame df, String col1, String col2){
        DataFrame ucdf = df.select(col1).distinct().unionAll(df.select(col2).distinct()).withColumnRenamed(col1,col1+" et "+col2);
        return ucdf;
    }
```

##Partie III

Dans cette partie, nous avons volontairement limité le nombre de lignes écrites par fichiers à 10.  
Les fichiers sont enregistrés dans le dossier <u>out/Partie3</u>  
Le format des fichiers est le suivant: 

- Partie3/Colonne1-Colonne2-Colonne3-Count
- Partie3/Colonne1-Colonne2-Colonne3-List

Pour optimiser le traitement du dataframe, on ne calcule pas toutes les paires possibles car elles sont inversibles.
```
for each colonne i
    for each colonne j where j != i
        for each colonne k where k != j != i and k > j
```
On utilise la fonction isC1SupC2(col1,col2) pour savoir
 si la colonne col1 à un indice supérieur à la colonne col2. Si c'est le cas, on ne calcule pas la paire <col1,col2>.

``` 
    public static boolean IsC1SupC2(String col1, String col2){

        String[] comp1 = col1.split("_");
        String[] comp2 = col2.split("_");

        System.out.println("Indice col1 : "+comp1[1]+" - Indice col2 : "+comp2[1]);

        if(Integer.parseInt(comp1[1]) > Integer.parseInt(comp2[1])){
            return true;
        }

        return false;
    }
```

##Partie IV

Dans cette partie, nous avons volontairement limité le nombre de lignes écrites par fichiers à 10.  
Les fichiers sont enregistrés dans le dossier <u>out/PartieIV</u>  
Le format des fichiers est le suivant: 

* Partie3/Colonne1-Colonne2-Colonne3-Count-fenetretemporelle
* Partie3/Colonne1-Colonne2-Colonne3-List-fenetretemporelle

On utilise le même code que pour la partie 4, mais on rajoute la boucle extérieure suivante :

```
    //On récupère la valeur maximale de temps

    DataFrame max = df.agg(max("_1_temps"));
    int m = Integer.parseInt(max.head().getString(0));
    
    //Pour chaque fenetre on récupère un dataframe de la taille de cette fenetre

    for(int bound = 0; bound+ft < m; bound+=ft) {
        DataFrame frame = df.where("_1_temps > "+bound+" and _1_temps < "+ (bound+ft));
```
