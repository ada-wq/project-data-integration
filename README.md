Documentation du Projet : Population Survey
1. Introduction
Le projet consiste à intégrer et analyser plusieurs jeux de données liés à l'American Community Survey de 2015. L'objectif est de traiter ces données, de les enrichir et de calculer des métriques en utilisant Kafka et Spark. Le fichier total-population.csv est transmis en temps réel via Kafka, tandis que les autres fichiers sont lus depuis le système de fichiers distribué (HDFS). Les données intégrées sont ensuite stockées sous forme de fichiers Parquet.
2. Technologies Utilisées
Pour ce projet, on a utilisé les technologies suivantes :
•	Apache Kafka : Pour la gestion du streaming des données, avec le fichier total-population.csv envoyé à un topic Kafka.
•	Apache Spark : Pour le traitement des données, l'intégration des différents fichiers CSV et le calcul des métriques. Spark Streaming a été utilisé pour consommer les données envoyées par Kafka.
•	Parquet : Format de stockage des données après traitement, pour garantir une lecture et une compression efficaces.
3. Processus d'Intégration des Données
3.1. Lecture des Données et Envoi via Kafka
1.	Kafka Producer :
o	Le fichier total-population.csv est découpé en lots de 100 lignes. Chaque lot est envoyé au topic Kafka toutes les 10 secondes.
o	Le producteur Kafka lit les lignes du fichier CSV et les envoie sous forme de messages à Kafka.
2.	Kafka Topic :
o	Un topic Kafka est utilisé pour recevoir les données envoyées par le producteur. Les données de population sont envoyées en streaming vers ce topic.



3.2. Consommation des Données avec Spark
1.	Spark Consumer :
o	On a configuré un consommateur Kafka avec Spark Streaming. Ce consommateur récupère les données envoyées par le producteur Kafka en temps réel.
o	Une fois les données reçues, elles sont jointes avec d'autres fichiers CSV (revenus des foyers, couverture santé, revenus d'auto-emploi) pour enrichir la base de données.
2.	Traitement des Données :
o	On a utilisé Spark pour effectuer les jointures entre les différentes sources de données. Cela permet d'enrichir les informations, telles que la population par catégorie d'âge, le revenu moyen par foyer, et d'autres métriques.
3.	Sauvegarde dans Parquet :
o	Après l'intégration, les données traitées sont sauvegardées dans un fichier Parquet, un format optimisé pour le stockage et la lecture rapide des grandes quantités de données.
Ces métriques ont été stockées dans des tables organisées pour une analyse ultérieure.
4. Gestion des Erreurs et Incohérences
4.1. Gestion des Erreurs
Le système est conçu pour gérer les erreurs et les incohérences dans les données de manière robuste :
1.	Données mal formatées : 
o	Si des données mal formatées sont reçues (par exemple, des valeurs manquantes ou des types de données incorrects), Spark les ignore ou les marque comme invalides.
2.	Erreurs d’envoi ou de consommation : 
o	Les erreurs d'envoi ou de consommation des messages Kafka sont gérées par des mécanismes de retries. En cas d’échec, le processus tente à nouveau d’envoyer ou de consommer le message.
3.	Reprise après un échec : 
o	Spark utilise les checkpoints pour garantir que le traitement peut reprendre après un échec. Si le processus échoue après avoir traité une partie des données, il redémarre à partir du dernier point de contrôle valide.
5. Exécution du Projet
5.1. Démarrage de Kafka
1.	Installation et démarrage de Kafka :
o	Kafka et Zookeeper ont été installés et configurés sur le cluster. On a démarré les services Kafka et Zookeeper via les commandes suivantes : 
o	./zookeeper-server-start.sh config/zookeeper.properties
o	./kafka-server-start.sh config/server.properties
2.	Création d’un topic Kafka :
o	Un topic Kafka a été créé pour recevoir les données en streaming : 
o	kafka-topics.sh --create --topic population-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

3.	Lancement du Consommateur Spark
spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark_consumer/spark_consumer.py
5.2. Exécution du Producteur Kafka
Le script kafka_producer.py a été exécuté pour envoyer les données de total-population.csv au topic Kafka.
5.3. Exécution du Consommateur Spark
Le script spark_consumer.py a été utilisé pour consommer les données depuis Kafka, effectuer les jointures nécessaires avec les autres fichiers CSV, et sauvegarder le résultat dans un fichier Parquet.
6. Conclusion
Le projet a été conçu pour être extensible et capable de traiter les données en temps réel. Le système d'intégration permet de garantir une reprise après un échec et offre une structure de données robuste grâce à l'utilisation de Kafka, Spark et Parquet. Les métriques calculées enrichissent les tables de données pour une analyse approfondie.
Ce projet peut être facilement reproduit, et des améliorations futures pourraient inclure l’ajout de nouveaux calculs ou l’optimisation de la gestion des erreurs.
Au final, on a consolidé les données provenant des quatre fichiers originaux en un seul fichier Parquet structuré. Ce fichier contient toutes les informations nécessaires de manière cohérente et optimisée avec la possibilité d’analyse et de visualisation
 
