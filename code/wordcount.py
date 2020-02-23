import sys
from operator import add
from pyspark.sql import SparkSession

if len(sys.argv) != 2:
    print("Usage: wordcount <file>", file=sys.stderr)
    sys.exit(-1)

# Récupération d'une SparkSession
spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

# Lecture d'un fichier texte : le fichier est décomposé en lignes
lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

# Décomposition de chaque ligne en mot (flatMap) pour associé à chaque mot une valeur (map)
# qui sera ensuite sommée avec les valeurs du même mot (reduceByKey).
word_counts = lines.flatMap(lambda line: line.split(' '))\
                   .map(lambda word: (word, 1))\
                   .reduceByKey(add)\
                   .collect()

# Chaque paire (clé, valeur) est affichée
for (word, count) in word_counts:
    print(word, count)
