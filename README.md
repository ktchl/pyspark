# pyspark

Mon dossier contient le fichier pyspark_TANCHALEUNE.py qui contient les requêtes des différentes questions

- J'ai utilisé la version 5.5 de mySql car la dernière version ne voulait pas se lancer avec docker




- Les étapes pour intégrer mes données dans MySQL:

docker run --name my-mysql -p 3306:3306 \
           -e MYSQL_ROOT_PASSWORD=my-secret-pw \
           -e MYSQL_USER=user \
           -e MYSQL_PASSWORD=password \
           -e MYSQL_DATABASE=database \
           -d mysql:5.5

spark-submit --driver-class-path mysql-connector-j-8.3.0.jar --jars mysql-connector-j-8.3.0.jar pyspark_TANCHALEUNE.py

mysql -h 127.0.0.1 -P 3306 -u root -p