import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classe principale pour l'analyse des logs avec Spark RDD en Java.
 */
public class LogAnalysisRDD {

    // Regex pour parser le format de log Apache Common Log Format étendu (simplifié)
    // Elle capture: IP, "-", Date, Méthode, Ressource, Protocole, Code, Taille
    private static final String LOG_ENTRY_PATTERN =
            "^(\\S+) - (\\S+) \\[(\\S+ \\+\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    /**
     * Classe pour encapsuler les champs extraits d'une ligne de log.
     */
    static class LogEntry implements Serializable {
        private String ipAddress;
        private String dateTime;
        private String httpMethod;
        private String resource;
        private int httpCode;
        private long responseSize;

        public LogEntry(String ip, String dt, String method, String res, int code, long size) {
            this.ipAddress = ip;
            this.dateTime = dt;
            this.httpMethod = method;
            this.resource = res;
            this.httpCode = code;
            this.responseSize = size;
        }

        // Getters
        public String getIpAddress() { return ipAddress; }
        public String getResource() { return resource; }
        public int getHttpCode() { return httpCode; }
    }

    /**
     * Comparateur sérialisable pour trier par compte décroissant
     */
    static class TupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {
        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            return o2._2.compareTo(o1._2); // Tri décroissant
        }
    }

    /**
     * Comparateur sérialisable pour trier par code HTTP croissant
     */
    static class CodeComparator implements Comparator<Tuple2<Integer, Long>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
            return o1._1.compareTo(o2._1); // Tri croissant par code
        }
    }

    public static void main(String[] args) {
        // Chemin par défaut si aucun argument n'est fourni
        String logFilePath;
        if (args.length < 1) {
            // Utiliser le chemin par défaut
            logFilePath = "src/main/resources/access.log";
            System.out.println("⚠️ Aucun argument fourni. Utilisation du chemin par défaut : " + logFilePath);
        } else {
            logFilePath = args[0];
        }

        // ----------------------------------------------------
        // ÉTAPE 1: Lecture des données et Initialisation Spark
        // ----------------------------------------------------
        SparkConf conf = new SparkConf().setAppName("LogAnalysisRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("\n--- 1. Lecture des données ---");
        JavaRDD<String> logLines = sc.textFile(logFilePath);
        System.out.println("✅ Fichier chargé dans le RDD.");

        // ----------------------------------------------------
        // ÉTAPE 2: Extraction des champs (Parsing)
        // ----------------------------------------------------
        System.out.println("\n--- 2. Extraction des champs (Parsing) ---");

        // RDD<String> -> RDD<LogEntry>
        JavaRDD<LogEntry> parsedLogs = logLines.map(
                (Function<String, LogEntry>) line -> {
                    Matcher matcher = PATTERN.matcher(line);
                    if (matcher.find()) {
                        try {
                            String ip = matcher.group(1);
                            String dt = matcher.group(3);
                            String method = matcher.group(4);
                            String res = matcher.group(5);
                            int code = Integer.parseInt(matcher.group(7));

                            // Gérer la taille de réponse "-" (souvent 0)
                            String sizeStr = matcher.group(8);
                            long size = sizeStr.equals("-") ? 0 : Long.parseLong(sizeStr);

                            return new LogEntry(ip, dt, method, res, code, size);
                        } catch (Exception e) {
                            // Log (en production) ou ignorer les lignes mal formatées
                            return null;
                        }
                    }
                    return null; // Ligne non parsée par la Regex
                }
        ).filter((Function<LogEntry, Boolean>) log -> log != null); // Filtrer les entrées nulles/non parsées

        // Optimisation: Cacher le RDD parsé pour les étapes suivantes
        parsedLogs.cache();

        // ----------------------------------------------------
        // ÉTAPE 3: Statistiques de base
        // ----------------------------------------------------
        System.out.println("\n--- 3. Statistiques de Base ---");

        long totalRequests = parsedLogs.count();
        System.out.println("Nombre total de requêtes : **" + totalRequests + "**");

        // Nombre total d'erreurs (codes HTTP >= 400)
        long totalErrors = parsedLogs.filter(
                (Function<LogEntry, Boolean>) log -> log.getHttpCode() >= 400
        ).count();
        System.out.println("Nombre total d'erreurs (codes >= 400) : **" + totalErrors + "**");

        // Pourcentage d'erreurs
        double errorPercentage = (totalRequests > 0) ? (double) totalErrors / totalRequests * 100 : 0.0;
        System.out.printf("Pourcentage d'erreurs : **%.2f%%**\n", errorPercentage);

        // ----------------------------------------------------
        // ÉTAPE 4: Top 5 des adresses IP
        // ----------------------------------------------------
        System.out.println("\n--- 4. Top 5 des Adresses IP ---");

        // MapToPair: (LogEntry) -> (IP, 1L)
        // ReduceByKey: Compte les requêtes par IP
        List<Tuple2<String, Long>> topIPs = parsedLogs.mapToPair(
                (PairFunction<LogEntry, String, Long>) log -> new Tuple2<>(log.getIpAddress(), 1L)
        ).reduceByKey(
                (Function2<Long, Long, Long>) (a, b) -> a + b
        ).takeOrdered(5, new TupleComparator());

        topIPs.forEach(tuple ->
                System.out.println("  IP: " + tuple._1 + ", Requêtes: " + tuple._2)
        );

        // ----------------------------------------------------
        // ÉTAPE 5: Top 5 des ressources les plus demandées
        // ----------------------------------------------------
        System.out.println("\n--- 5. Top 5 des Ressources Demandées ---");

        // MapToPair: (LogEntry) -> (Ressource, 1L)
        // ReduceByKey: Compte les requêtes par ressource
        List<Tuple2<String, Long>> topResources = parsedLogs.mapToPair(
                (PairFunction<LogEntry, String, Long>) log -> new Tuple2<>(log.getResource(), 1L)
        ).reduceByKey(
                (Function2<Long, Long, Long>) (a, b) -> a + b
        ).takeOrdered(5, new TupleComparator());

        topResources.forEach(tuple ->
                System.out.println("  Ressource: " + tuple._1 + ", Nombre: " + tuple._2)
        );

        // ----------------------------------------------------
        // ÉTAPE 6: Répartition des requêtes par code HTTP
        // ----------------------------------------------------
        System.out.println("\n--- 6. Répartition des Requêtes par Code HTTP ---");

        // MapToPair: (LogEntry) -> (Code HTTP, 1L)
        // ReduceByKey: Compte les requêtes par code
        List<Tuple2<Integer, Long>> codeDistribution = parsedLogs.mapToPair(
                (PairFunction<LogEntry, Integer, Long>) log -> new Tuple2<>(log.getHttpCode(), 1L)
        ).reduceByKey(
                (Function2<Long, Long, Long>) (a, b) -> a + b
        ).collect(); // Collecte tous les codes

        // Tri par code HTTP pour l'affichage
        codeDistribution.stream()
                .sorted(new CodeComparator())
                .forEach(tuple ->
                        System.out.println("  Code HTTP " + tuple._1 + ": " + tuple._2 + " requêtes")
                );

        System.out.println("\n✅ Analyse terminée avec succès!");

        // Arrêt du contexte Spark
        sc.stop();
    }
}