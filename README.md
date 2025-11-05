Markdown

# 💻 TP1 : Programmation des RDDs avec Apache Spark

## Table des Matières
* [🚀 Introduction](#-introduction)
* [🎯 Objectifs du TP](#-objectifs-du-tp)
* [⚙️ Technologies Utilisées](#-technologies-utilisées)
* [📂 Structure du Projet](#-structure-du-projet)
* [💡 Exercice 1 : Analyse des Ventes (Données Structurées)](#-exercice-1--analyse-des-ventes-données-structurées)
* [📊 Exercice 2 : Analyse de Fichiers de Logs (Données Semi-structurées)](#-exercice-2--analyse-de-fichiers-de-logs-données-semi-structurées)
* [▶️ Comment Exécuter le Projet](#-comment-exécuter-le-projet)

---

## 🚀 Introduction

Ce projet est le **Travail Pratique n°1** sur la programmation distribuée avec **Apache Spark**. Il vise à maîtriser l'utilisation des **RDD (Resilient Distributed Datasets)** pour le traitement de données massives (**Big Data**).

Nous explorons deux cas d'usage fondamentaux : l'agrégation de données structurées de ventes, et le *parsing* et l'analyse de fichiers de logs Apache. L'objectif est de démontrer l'efficacité des transformations (`map`, `filter`, `flatMap`) et des actions (`reduceByKey`, `count`) des RDDs en environnement distribué.

---

## 🎯 Objectifs du TP

* Comprendre et implémenter le concept de **RDD** de Spark.
* Appliquer les transformations `map`, `filter`, `reduceByKey` pour l'agrégation de données.
* Développer des applications Spark en mode **local**.
* Traiter différents formats de données : structuré (ventes) et semi-structuré (logs).
* Calculer des indicateurs clés (KPIs) : totaux, pourcentages d'erreurs, Top N.

---

## ⚙️ Technologies Utilisées

| Technologie | Version | Description |
| :--- | :--- | :--- |
| **Apache Spark** | [À compléter : ex. 3.4.0] | Moteur d'analyse unifié pour le traitement de données à grande échelle. |
| **Langage de Dev.** | **Java** | Langage utilisé pour la programmation des applications Spark. |
| **Système de Build** | [À compléter : ex. Maven/Gradle] | Pour la gestion des dépendances et la construction du JAR. |

---

## 📂 Structure du Projet
<img width="702" height="685" alt="image" src="https://github.com/user-attachments/assets/d4ee22b7-6372-4e55-9e29-e3d9de307fcb" />


---

## 💡 Exercice 1 : Analyse des Ventes (Données Structurées)

### Fichier d'entrée
`data/ventes.txt` (Structure : `date ville produit prix`)

### Travaux Réalisés
1.  **Total des ventes par ville** : Calcul de la somme des prix pour chaque ville unique en utilisant `map` et `reduceByKey`.
2.  **Prix total des ventes par ville et par année** : Extension du travail précédent pour inclure l'année dans la clé d'agrégation.

---

## 📊 Exercice 2 : Analyse de Fichiers de Logs (Données Semi-structurées)

### Fichier d'entrée
`data/access.log` (Format Apache combiné)

### Travaux Réalisés
1.  **Extraction de champs** : Implémentation d'une fonction de *parsing* robuste (via Regex) pour isoler les 6 champs demandés (IP, date/heure, méthode, ressource, code HTTP, taille de la réponse).
2.  **Statistiques de base** : Calcul du nombre total de requêtes et du pourcentage de requêtes en erreur (codes $\geq 400$).
3.  **Top N** : Détermination des 5 adresses IP les plus actives et des 5 ressources les plus demandées.
4.  **Répartition par code HTTP** : Comptage du nombre de requêtes pour chaque code de statut HTTP.

---

## ▶️ Comment Exécuter le Projet

### Prérequis
* Installation de **Java JDK** (*[À compléter : ex. 17]*).
* Installation et configuration de **Apache Spark** (*[À compléter : assurez-vous que `spark-submit` est disponible]*).
* Outil de build : **Maven** ou **Gradle**.

### Étapes d'exécution

1.  **Cloner le dépôt :**
    ```bash
    git clone [URL_DE_VOTRE_REPO]
    cd [NOM_DU_REPO]
    ```

2.  **Construire le fichier JAR :**
    *(Exemple avec Maven)*
    ```bash
    mvn clean package
    ```

3.  **Lancer l'application :**
    Utilisez `spark-submit` pour exécuter les classes Java.

    *Exemple pour l'Analyse des Logs :*
    ```bash
    spark-submit \
      --class Ex2_Logs.AnalyseLogs \
      --master local[*] \
      target/[NOM_DU_FICHIER_JAR].jar \
      ./data/access.log
    ```
    *(Ajustez le `--class` et le nom du fichier JAR en fonction de vos conventions d
