# Tutoriel d'une technologie émergente : Spark

Ce tutoriel a pour objectif de faire découvrir la technologie [Spark](https://spark.apache.org/).\
Il s'inscrit dans le cadre du cours [8INF853 - Architecture des applications d'entreprise](https://cours.uqac.ca/8INF853) dispensé par Fabio Petrillo pour le programme de [Maîtrise en informatique](https://www.uqac.ca/programme/3037-maitrise-en-informatique/) à l'UQAC.  

## Présentation de Spark

Apache Spark est un **framework open source** de **calcul distribué**.\
Développé à l’université de Californie à Berkeley en 2014, Spark est aujourd’hui un projet de la fondation Apache.\ Il s'agit essentiellement d'un cadre applicatif de traitements **big data** pour effectuer des analyses complexes à grande échelle.\
Spark permet d’optimiser l’utilisation d’un **cluster** pour des opérations d'analyses et permet ainsi de minimiser le temps requis pour obtenir les résultats.\
Ce framework fournit plusieurs composants et librairies aux utilisateurs pour le traitement de données :

### Contexte

Pourquoi avons-nous besoin d’un nouveau moteur de calculs et d’un modèle de programmation distribué pour l’analyse de données ?\
Comme plusieurs autres changements dans le monde celui-ci provient en partie de raisons économiques liées aux applications ainsi qu’au matériel informatique.\

Historiquement les processeurs des ordinateurs devenaient de plus en plus rapides d’année en année. Ainsi par défaut les applications construites sur ceux-ci devenaient également plus rapides. Ceci a mené a l’établissement d’un large écosystème d’applications conçues pour être exécutées principalement sur un seul processeur.\

Cette tendance prit malheureusement fin vers 2005 en raison des limitations au niveau de la dissipation de chaleur liée à l’augmentation de cadence des processeurs. À partir de ce moment, les manufacturiers ont plutôt choisi d’ajouter plusieurs cœurs au processeur. Ceci a pour effets de modifier les patterns de création d’application pour utiliser un modèle à plusieurs processus.

Durant ce même laps de temps, les technologies do stockage et d'acquisition de données n’ont pas subi la même pression que les processeurs. De ce fait, le coût de stockage et des technologies d’acquisition de données ont considérablement diminué (camera, capteur, IOT, etc.). Ceci a donc causé une explosion de la quantité de données disponibles prêtes à être analyser.\

Cette quantité astronomique de données à analyser a donc générer un nouveau besoin : celui d'une grande plateforme de calculs distribués telle que Spark.

### Historique

- 2009 : Conception de Spark par Matei Zaharia lors de son doctorat a l’université de Californie à Berkeley. À l’origine la solution a pour but d’accélérer le traitement des systèmes Hadoop.
- 2013 : Transmission de Spark a la fondation Apache. Il devient alors l’un des projets les plus actifs de la fondation.
- 2014 : Spark remporte le Daytona GraySort Contest (trier 100 To de donnés le plus rapidement possible). Le record, détenu préalablement par Hadoop (72 minutes), est largement battu avec un temps d'exécution de seulement 23 minutes. Spark démontre ainsi sa puissance en étant 3 fois plus rapide et en utilisant approximativement 10 fois moins de machines qu'Hadoop (206 contre 2100).

### Philosophie

La philosophie de base de Apache Spark est de fournir un moteur de calcul unifié et une collection de librairies pour le **big data**.

#### Plateforme unifié

Le but est de fournir une plateforme pour la création d’application big data. Spark est désigné pour supporter une grande variété de tâches liées à l’analyse de données que ce soit l'importation de données et des requêtes SQL mais également des tâches d’apprentissage automatique et des calculs sur des flux (streaming). Toutes ces tâches sont réalisées à partir du même moteur de calcul avec un ensemble d’API consistant. Ces API consistants ont également comme fonction l’optimisation des calculs. Ainsi si vous fait une importation de données via une requête SQL et ensuite vous demandez l’exécution d’un calcul d’apprentissage automatique sur le même jeu de données. Le moteur de Spark va combiner les 2 opérations et tenter d’optimiser le plus possible les étapes pour fournir l’exécution la plus performante possible.\

Spark est créé en Scala, il supporte également Java, Python, SQL et R. certaines distinctions existent en fonction des langages utiliser, mais l’essentiel est disponible pour ceux-ci. Puisque Spark fut créé en Scala, ce dernier est le langage de programmation le plus puissant pour l'utilisation de Spark puisqu’il permet d’atteindre les API de bas niveau plus efficacement.

#### Moteur de calcul

Tout en tentant de rester unifié, Spark limite le plus possible l’étendue de son moteur de calcul. Dans ce sens, il prend en change l’importation des données et le calcul en mémoire mais ne prend par contre pas en charge le stockage permanent des données après les calculs. Cependant, Spark reste compatible avec une grande variété de produits de stockage tels que : 
- Systèmes de stockage sur le Cloud : Azure Storage et Amazon S3
- Systèmes de fichiers distribués : Apache Hadoop
- Bases de données NoSQL clé-valeur : Apache Cassandra
- Agents de messages : Apache Kafka

Spark ne fait que les utiliser, il ne stocke rien pour ses calculs et ne préfère aucune solution en particulier. L’idée derrière ceci est que les données utilisées pour les calculs existent déjà à travers une multitude de solutions de stockage. Le déplacement de données est un mécanisme coûteux, c'est pourquoi Spark se concentre sur l'exécution des calculs sur les données et ceci peu importe leurs emplacements. Ce point est en fait ce qui le distingue des plateformes précédentes telles qu’Apache Hadoop. Cette plateforme offrait le stockage (Hadoop File System), le moteur de calcul (MapReduce) et le service de cluster. Ceci limitait ainsi les traitements sur les données stockées sur d'autres solutions de stockage.

#### Librairies

Le dernier composant est les librairies qui ont été crées de façon unifiée pour tirer profit du moteur de calcul. Spark supporte les librairies livrées avec le produit ainsi que celles crées par la communauté. Les librairies sont en quelques sortes l’un des aspects les plus importants du projet et fournissent de plus en plus de fonctionnalités à Spark. Actuellement, les librairies offrent les fonctionnalités de : 
- Requête sur des données structurées (Spark SQL)
- Apprentissage automatique (MLib)
- Traitement en continu de flux de données
- Analyse de graphe (GraphX)
- Des centaines d’autres connecteurs et librairies...

### Architecture




## Auteurs
Quentin LETORT - LETQ12039703\
Guillaume ROUTHIER - ROUG10088306

## Références
- [Wikipédia - Apache Spark](https://fr.wikipedia.org/wiki/Apache_Spark)
- [Spark : The Definitive Guide. Bill Chambers & Matei Zaharia. O’Reilly Media 2018.](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Documentation officielle - Apache Spark](https://spark.apache.org/docs/latest/)
- [Open Classrooms - Réalisez des calculs distribués sur des données massives](https://openclassrooms.com/fr/courses/4297166-realisez-des-calculs-distribues-sur-des-donnees-massives/4308661-allez-au-dela-de-mapreduce-avec-spark)
