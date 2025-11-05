package org.sid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.util.List;

public class SalesByCityYear {
    public static void main(String[] args) {

        // --- 1. Configuration et Initialisation ---

        // Configuration de l'utilisateur Hadoop (souvent nécessaire en environnement distribué)
        System.setProperty("HADOOP_USER_NAME", "hajar");

        // Chemin du fichier d'entrée (argument ou par défaut)
        // Assurez-vous que 'ventes.txt' est un fichier texte dont les colonnes sont séparées par des espaces.
        String inputPath = args.length > 0 ? args[0] : "src/main/resources/ventes.txt";

        // Configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total ventes par ville et par annee")
                .setMaster("local[*]"); // 'local[*]' pour utiliser tous les cœurs disponibles localement
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Réduire les logs de Spark (pour une sortie plus propre)
        sc.setLogLevel("ERROR");

        System.out.println("\n*** Démarrage du traitement Spark sur le fichier : " + inputPath + " ***\n");

        // --- 2. Chargement et Nettoyage des Données ---

        // Lecture du fichier
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Filtrage des lignes vides et en-têtes (supposant que l'en-tête commence par 'Date')
        JavaRDD<String> dataLines = lines
                .filter(line -> line != null && !line.trim().isEmpty())
                .filter(line -> !line.trim().toLowerCase().startsWith("date"));

        // --- 3. Transformation (Map) : Création des paires (clé, valeur) ---

        // Transformation : Map chaque ligne en paire ((ville_année), prix)
        JavaPairRDD<String, Double> cityYearPricePairs = dataLines.mapToPair(line -> {
            String[] parts = line.trim().split("\\s+"); // Séparateur : un ou plusieurs espaces

            // Vérification minimale des champs
            if (parts.length < 4) {
                // Clé pour les lignes invalides (pour pouvoir les filtrer ou les analyser après)
                return new Tuple2<>("INVALID_LINE_0", 0.0);
            }

            String dateStr = parts[0];
            String ville = parts[1];
            // Le 3ème champ (parts[2]) est ignoré si le format est : Date Ville Produit Prix
            String prixStr = parts[3];

            // Conversion du prix
            double prix;
            try {
                prix = Double.parseDouble(prixStr);
            } catch (NumberFormatException e) {
                prix = 0.0; // Prix invalide ignoré
            }

            // Extraction de l'année
            String year = "UNKNOWN_YEAR"; // Valeur par défaut plus explicite
            try {
                LocalDate dt = LocalDate.parse(dateStr); // Tente de parser au format ISO (YYYY-MM-DD)
                year = String.valueOf(dt.getYear());
            } catch (Exception ex) {
                // Fallback: essayer de prendre les 4 premiers caractères comme l'année
                if (dateStr.length() >= 4 && dateStr.substring(0, 4).matches("\\d{4}")) {
                    year = dateStr.substring(0, 4);
                } else {
                    ville = "INVALID_DATE_" + ville; // Marquer la ville si la date est totalement inutilisable
                }
            }

            // Création de la clé combinée
            String key = ville + "_" + year;

            return new Tuple2<>(key, prix);
        });

        // --- 4. Agrégation (Reduce) : Somme des prix par clé ---

        // Réduction : Somme des prix par (ville, année)
        JavaPairRDD<String, Double> sumByCityYear = cityYearPricePairs.reduceByKey((a, b) -> a + b);

        // Filtrer les entrées invalides (si elles existent)
        JavaPairRDD<String, Double> filteredResults = sumByCityYear
                .filter(tuple -> !tuple._1().startsWith("INVALID_"));

        // Tri des résultats par clé (Ville_Année)
        JavaPairRDD<String, Double> sortedResults = filteredResults.sortByKey();

        // --- 5. Collecte et Affichage des Résultats ---

        // Collecte des résultats
        List<Tuple2<String, Double>> results = sortedResults.collect();

        // Affichage formaté
        System.out.println("\n" + "=".repeat(60));
        System.out.println("          TOTAL VENTES PAR VILLE ET PAR ANNÉE");
        System.out.println("=".repeat(60));

        for (Tuple2<String, Double> result : results) {
            // Split la clé "Ville_Année"
            String[] parts = result._1().split("_", 2);
            String ville = parts.length > 0 ? parts[0] : "UNKNOWN";
            String year = parts.length > 1 ? parts[1] : "N/A";

            // Affichage soigné
            System.out.printf("%-20s | %s  ->  %12.2f DH\n", ville, year, result._2());
        }

        System.out.println("=".repeat(60) + "\n");

        // Option : Sauvegarder dans un fichier (décommenter si besoin)
        // sortedResults.saveAsTextFile("output/sum_by_city_year_" + System.currentTimeMillis());

        // --- 6. Nettoyage ---

        // Fermeture du contexte Spark
        sc.close();
    }
}