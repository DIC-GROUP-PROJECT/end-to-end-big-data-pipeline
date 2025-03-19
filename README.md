# 0. Install
Make sure you have docker installed on your own computer.
[](https://docs.docker.com/get-started/get-docker/ )
# 1. How to start the Hadoop Cluster

```bash
docker compose -f docker-compose.yaml up -d
```

After that, you will get some output like this:
```
[+] Running 5/5
 ✔ Network project_default              Created                                                                                                                                                           0.1s 
 ✔ Container project-resourcemanager-1  Started                                                                                                                                                           0.6s 
 ✔ Container project-namenode-1         Started                                                                                                                                                           0.5s 
 ✔ Container project-datanode1-1        Started                                                                                                                                                           0.6s 
 ✔ Container project-nodemanager1-1     Started 
```

# 2. Upload file to HDFS

Now you can get into your hadoop namenode container by such command:

```bash
docker exec -it project-namenode-1 bash
```

Now you are in the `/opt/hadoop` path, which contains all the hadoop things.
And execute `cat README.txt`, you can get some text content:

```
For the latest information about Hadoop, please visit our website at:

   http://hadoop.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/HADOOP/
```

Now, let's upload this file to hdfs

```bash
hdfs dfs -mkdir /input
hdfs dfs -put README.txt /input/wc.txt
```

You can check the file on the hdfs by 

```bash
hdfs dfs -cat /input/wc.txt
```

You can also open your web browser and go to `http://localhost:9870/explorer.html#/`, which is a built-in file explorer

# 3. Data Ingestion Script
Please refer to the `dataingestion`, it is a Java application.
You can use any IDE you want, like `VSCode` or `IntelliJ IDEA`. 
There is a very simple example for you to write/read data from HDFS.

If you want to execute the script on your local machine, please add these hosts to your `/etc/hosts`
```
127.0.0.1 namenode
127.0.0.1 datanode1
127.0.0.1 resourcemanager
127.0.0.1 nodemanager
```

If you want to build an executable file, please run this command:
```shell
./gradlew clean shadowJar
```

If you want to run this executable file on the namenode, please run this command:
```shell
docker cp ./app/build/libs/app-all.jar  <your namenode container name>:/opt/hadoop/app-all.jar

docker exec -it <your namenode container name> bash

# on the namenode
java -jar app-all.jar
```


# 4. Spark Example

Create a python file by using `vi spark.py`. If you don't know what `vi` is, please refer to this [online course](https://missing.csail.mit.edu/2020/editors/)


```python
from pyspark import SparkConf, SparkContext
def main():

    conf = SparkConf().setAppName("WordCountDemo")
    sc = SparkContext(conf=conf)
    
    input_path = "hdfs://namenode/input/wc.txt"
    output_path = "hdfs://namenode/output/wordcount_result"

    text_file = sc.textFile(input_path)
    
    counts = (text_file
              .flatMap(lambda line: line.split())
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))
    
    counts.saveAsTextFile(output_path)
    
    sc.stop()

if __name__ == "__main__":
    main()

```

And you can run this by `./spark/bin/spark-submit  --master yarn   --deploy-mode cluster spark.py`

If you success, you can check the result in `/output/wordcount_result`




