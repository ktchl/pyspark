#Q1
from urllib.request import urlretrieve

def download_file(url,filename):
    urlretrieve(url,filename)
    
download_file("https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_app.csv","gps_app.csv")
download_file("https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_user.csv","gps_user.csv")

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf

conf = SparkConf()
conf.set("spark.log.level", "error") 
conf.set("spark.ui.showConsoleProgress", "false") 

sc = SparkContext.getOrCreate(conf=conf) 

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("exam_Spark") \
    .getOrCreate()

spark

from pyspark.sql.types import *
schema_raw_app = StructType([StructField("App",StringType(),True),
                             StructField("Category",StringType(),True),
                             StructField("Rating",DoubleType(),True),
                             StructField("Reviews",StringType(),True),
                             StructField("Size",StringType(),True),
                             StructField("Installs",StringType(),True),
                             StructField("Type",StringType(),True),
                             StructField("Price",StringType(),True),
                             StructField("Content Rating",StringType(),True),
                             StructField("Genres",StringType(),True),
                             StructField("Last Updated",StringType(),True),
                             StructField("Current Ver",StringType(),True),
                             StructField("Android Ver",StringType(),True)
                            ])

schema_raw_user = StructType([StructField("App",StringType(),True),
                              StructField("Translated_Review",StringType(),True),
                              StructField("Sentiment",StringType(),True),
                              StructField("Sentiment_polarity",StringType(),True),
                              StructField("Sentiment_Subjectivity",StringType(),True)
                             ])

raw_app = spark.read.option("header", True)\
                    .schema(schema_raw_app)\
                    .option("escape", "\"")\
                     .option("charset", "UTF-8")\
                    .csv("gps_app.csv")

raw_user = spark.read.option("header", True)\
                     .schema(schema_raw_user)\
                     .option("escape", "\"")\
                      .option("charset", "UTF-8")\
                     .csv("gps_user.csv")


#Q2: Dans un premier prétraitement, renommer toutes les colonnes en remplaçant les espaces par des soulignements et les majuscules par des minuscules.
def rename_columns(df):
    columns_rename = {}
    for colonne in df.columns:
        columns_rename.update({colonne:colonne.replace(" ","_").lower()})
    df = df.withColumnsRenamed(columns_rename)
    df.printSchema()
    return df
from pyspark.sql.functions import *   
raw_app = rename_columns(raw_app)
raw_user = rename_columns(raw_user)

#J'ai enlevé les caractères qui ne sont pas ut8 afin de pouvoir les intégrer dans mySQL car la version que j'utilise ne prends pas en charge les caractères autre que ut8
def remove_no_utf8(df,colonne):
   return df.withColumn(colonne, regexp_replace(colonne, "[^\x00-\x7F]+", ""))
raw_app = remove_no_utf8(raw_app,"app")
raw_user = remove_no_utf8(raw_user,"translated_review")
raw_user = remove_no_utf8(raw_user,"app")



#Q3.1: Remplacer les valeurs manquantes dans la colonne rating par la moyenne ou la médiane. Justifier le choix.
#J'ai choisi la moyenne des notes car cela semble plus cohérente par rapport à nos données des applications
from pyspark.sql.functions import *
avg_rating = raw_app.filter(col("rating") != "NaN").select(avg(col("rating"))).collect()[0][0]
raw_app = raw_app.na.fill({"rating": avg_rating})

#Q3.2: Remplacer la valeur manquante de la colonne type par la valeur la plus logique. Justifier le choix.
#J'ai choisi de remplacer la valeur manquante par la valeur Free car c'est la valeur la plus présente.
raw_app = raw_app.withColumn("type", when(col("type")=="NaN", raw_app.groupBy("type").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(1).collect()[0][0]).otherwise(col('type')))

#Q3.3: Afficher les valeurs uniques prisent par la colonne type. Que remarquez-vous ? Supprimer le problème. Cela réglera aussi la valeur manquante de la colonne content_rating.
#Je remarque qu'il y a une valeure 0 dans la colonne type
raw_app.select(col("type")).distinct().show()
raw_app = raw_app.filter(col("type") != "0")

#Q3.4: Remplacer le reste des valeurs manquantes pour la colonne current_ver et la colonne android_ver par leur modalité respective.
raw_app = raw_app.withColumn("current_ver", when((col("current_ver")=="NaN") | (col("current_ver").isNull()), raw_app.groupBy("current_ver").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(1).collect()[0][0]).otherwise(col('current_ver')))
raw_app = raw_app.withColumn("android_ver", when((col("android_ver")=="NaN") | (col("android_ver").isNull()), raw_app.groupBy("android_ver").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(1).collect()[0][0]).otherwise(col('android_ver')))

#Q3.5: Vérifier qu'il ne reste plus de valeurs manquantes grâce à l'application de la commande :
#J'ai repris les fonctions du cours
import numpy as np
def getMissingValues(dataframe):
  count = dataframe.count()
  columns = dataframe.columns
  nan_count = []
  # we can't check for nan in a boolean type column
  for column in columns:
    if dataframe.schema[column].dataType == BooleanType():
      nan_count.append(0)
    else:
      nan_count.append(dataframe.where(isnan(col(column))).count())
  null_count = [dataframe.where(isnull(col(column))).count() for column in columns]
  return([count, columns, nan_count, null_count])

def missingTable(stats):
  count, columns, nan_count, null_count = stats
  count = str(count)
  nan_count = [str(element) for element in nan_count]
  null_count = [str(element) for element in null_count]
  max_init = np.max([len(str(count)), 10])
  line1 = "+" + max_init*"-" + "+"
  line2 = "|" + (max_init-len(count))*" " + count + "|"
  line3 = "|" + (max_init-9)*" " + "nan count|"
  line4 = "|" + (max_init-10)*" " + "null count|"
  for i in range(len(columns)):
    max_column = np.max([len(columns[i]),\
                        len(nan_count[i]),\
                        len(null_count[i])])
    line1 += max_column*"-" + "+"
    line2 += (max_column - len(columns[i]))*" " + columns[i] + "|"
    line3 += (max_column - len(nan_count[i]))*" " + nan_count[i] + "|"
    line4 += (max_column - len(null_count[i]))*" " + null_count[i] + "|"
  lines = f"{line1}\n{line2}\n{line1}\n{line3}\n{line4}\n{line1}"
  print(lines)

missingTable(getMissingValues(raw_app))

#Q4.1: Étudier les valeurs manquantes présents dans ce jeu de données. Les valeurs manquantes (nan) de chaque colonne sont-elles toutes sur les mêmes lignes ?
#Oui les valeurs manquantes de chaques colonnes sont sur les mêmes lignes donc on les supprime. Je supprime aussi les 5 lignes où les valeurs sont manquantes dans la colone translated_review
missingTable(getMissingValues(raw_user))
raw_user.filter(col("translated_review") == "nan").show()

#Q4.2: Nettoyer les valeurs manquantes.
raw_user = raw_user.filter((col("translated_review") != "nan"))

#Q4.3: Vérifier qu'il ne reste plus de valeurs manquantes grâce à l'application de la commande :
missingTable(getMissingValues(raw_user))

#Q5.1: Vérifier si il reste des valeurs non numériques dans les colonnes sentiment_polarity et sentiment_subjectivity. Pour se faire on pourra filtrer les lignes pour lesquelles transformer la colonne en double renvoie une valeur manquante.
#Aucune valeurs non numérique
raw_user.filter(col("sentiment_polarity").astype(DoubleType()).isNull()).show()
raw_user.filter(col("sentiment_polarity").astype(DoubleType()) == "nan").show()
raw_user.filter(col("sentiment_subjectivity").astype(DoubleType()).isNull()).show()
raw_user.filter(col("sentiment_subjectivity").astype(DoubleType()) == "nan").show()

#Q5.2: Convertir les colonnes numériques au format float.
raw_user = raw_user.withColumn("sentiment_polarity", col("sentiment_polarity").astype(FloatType()))
raw_user = raw_user.withColumn("sentiment_subjectivity", col("sentiment_subjectivity").astype(FloatType()))
raw_user.printSchema()

#Q5.3: Remplacer les caractères spéciaux de la colonne translated_review par des espaces. Remplacer ensuite tous les espaces de taille supérieure à 2 par un espace de taille 1. Pour répondre à cette question on pourra utiliser la fonction regexp_replace de la collection pyspark.sql.functions.
raw_user = raw_user.withColumn("translated_review", regexp_replace(col("translated_review"), r"[\"!.,@#&*%\']", " "))
raw_user = raw_user.withColumn("translated_review", regexp_replace(col("translated_review")," {2,}"," "))

#Q5.4: Minimiser tous les caractères de la colonne translated_review.
raw_user = raw_user.withColumn("translated_review", lower(col("translated_review")))

#Q5.5: Afficher le nombre de commentaires pour chacun des groupes de tailles allant de 1 caractère à 10 caractères.
raw_user.filter((length(col("translated_review")) >=1)  & (length(col("translated_review")) <= 10)).show()

#Q5.6: Conserver uniquement les lignes dont le commentaire est de taille supérieure ou égale à 3.
raw_user = raw_user.filter(length(col("translated_review")) >=3)

#Q5.7: Calculer les 20 mots les plus présents pour les commentaires étant positifs. Pour se faire on pourra passer par l'attribut rdd du DataFrame puis extraire la colonne translated_review avant d'appliquer le MapReduce déjà vu dans le chapitre 2.
rdd_raw_user = raw_user.select(col("translated_review")).filter(col("sentiment") =="Positive").rdd
rdd20 = rdd_raw_user.flatMap(lambda x : x.translated_review.split()).map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x : x[1],False)
rdd20.filter(lambda x: len(x[0]) >=3).take(20)

#Q6.1: Changer le type de la colonne reviews en integer en transformant les lignes problématiques si nécessaire.
raw_app.select(col("reviews")).filter(col("reviews").isNull()).show()
raw_app.select(col("reviews")).filter(col("reviews")== "NaN").show()
raw_app.select(col("reviews")).filter(col("reviews")== "nan").show()
raw_app = raw_app.withColumn("reviews", col("reviews").astype(IntegerType()))
raw_app.printSchema()

#Q6.2: Nous allons maintenant convertir la colonne installs en integer aussi. Pour se faire on va utiliser une regex assez similaire à celles utilisées précédemment afin de remplacer tous les caractères n'étant pas des chiffres par un vide. On pourra, avant de remplacer la colonne, s'assurer qu'il n'y a pas de valeurs nulles.
raw_app.select(col("installs")).filter(col("installs").isNull()).show()
raw_app.select(col("installs")).filter(col("installs")== "NaN").show()
raw_app.select(col("installs")).filter(col("installs")== "nan").show()
raw_app = raw_app.withColumn("installs", regexp_replace(col("installs"), r"[^\d]", "").astype(IntegerType()))
raw_app.show(1)

#6.3: Répéter le même type d'opération pour transformer la colonne price en double. Attention ici à bien traiter les nombres à virgule.
raw_app.select(col("price")).filter(col("price").isNull()).show()
raw_app.select(col("price")).filter(col("price")== "NaN").show()
raw_app.select(col("price")).filter(col("price")== "nan").show()
raw_app = raw_app.withColumn("price", regexp_replace(col("price"), r"[$]", "").astype(DoubleType()))

#Q6.4: En partant du principe que la date de la colonne last_updated est au format MMMM d, yyyy, convertir cette colonne au format date avec la fonction to_date.
raw_app = raw_app.withColumn("last_updated",to_date(col("last_updated"), "MMMM d, yyyy").alias("last_updated").astype(DateType()))

#Insertion des données dans MySQL
raw_app.write \
       .mode('overwrite') \
       .format("jdbc") \
       .option("url", "jdbc:mysql://localhost:3306/database?useUnicode=true&characterEncoding=utf8") \
       .option("dbtable", "gps_app") \
       .option("user", "user") \
       .option("password", "password") \
       .save()

raw_user.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/database?useUnicode=true&characterEncoding=utf8") \
        .option("dbtable", "gps_user") \
        .option("user", "user") \
        .option("password", "password") \
        .save()