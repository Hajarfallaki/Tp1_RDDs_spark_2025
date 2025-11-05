package org.sid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
// Importation de UserGroupInformation retirée pour éviter l'erreur "getSubject"
import java.util.List;

public class SalesByCity {
    public static void main(String[] args) {
        // Solution pour l'erreur de sécurité : définir l'utilisateur Hadoop manuellement
        // C'est l'étape cruciale pour éviter l'UnsupportedOperationException en local.
        System.setProperty("HADOOP_USER_NAME", "hajar");

        // Chemin du fichier en argument ou valeur par défaut
        String inputPath = args.length > 0 ? args[0] : "src/main/resources/ventes.txt";

        SparkConf conf = new SparkConf()
                .setAppName("Total ventes par ville")
                .setMaster("local[*]"); // Tester localement avec tous les cœurs
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Optionnel : Réduire le niveau de log pour ne voir que les erreurs et éviter le bruit.
        sc.setLogLevel("ERROR");

        // Lire le fichier texte
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Filtrer lignes vides et ignorer l'éventuel header (ligne commençant par "date")
        JavaRDD<String> dataLines = lines
                .filter(line -> line != null && !line.trim().isEmpty())
                .filter(line -> !line.trim().toLowerCase().startsWith("date"));

        // Transformer en paires (Ville, Prix)
        // Chaque ligne attendue : date ville produit prix
        JavaPairRDD<String, Double> cityPricePairs = dataLines.mapToPair(line -> {
            // Utiliser une expression régulière pour séparer par un ou plusieurs espaces
            String[] parts = line.trim().split("\\s+");

            // Vérification simple de la structure de la ligne
            if (parts.length < 4) {
                // Retourne UNKNOWN si la ligne n'a pas assez de parties
                return new Tuple2<>("UNKNOWN", 0.0);
            }

            String ville = parts[1];
            double prix;
            try {
                // Le prix est supposé être le 4ème élément (index 3)
                prix = Double.parseDouble(parts[3]);
            } catch (NumberFormatException e) {
                // En cas d'erreur de conversion (si ce n'est pas un nombre)
                prix = 0.0;
            }
            return new Tuple2<>(ville, prix);
        });

        // Réduction par clé : sommer tous les prix pour chaque ville
        JavaPairRDD<String, Double> sumByCity = cityPricePairs.reduceByKey(Double::sum);

        // Collecter les résultats sur le pilote et afficher
        List<Tuple2<String, Double>> results = sumByCity.collect();
        System.out.println("=== Total ventes par ville ===");
        results.forEach(t -> System.out.println(t._1() + " -> " + t._2()));

        // Fermer le contexte Spark
        sc.close();
    }
}