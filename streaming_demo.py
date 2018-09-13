import sys

from pyspark import SparkContext                # allow us to work with spark
from pyspark.streaming import StreamingContext  # allow to work with streams in spark

if __name__ == "__main__":
    sc = SparkContext("local[2]", "StreamingCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)  # 2 is a batchInterval prop of the DStream created by this StreamingContext

    ssc.checkpoint('file:///tmp/spark')

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))  # hostname and port

    counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

# ncat -lk 9999
# spark-submit .\streaming_demo.py localhost 9999
