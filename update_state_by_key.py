import sys

from pyspark import SparkContext                # allow us to work with spark
from pyspark.streaming import StreamingContext  # allow to work with streams in spark

if __name__ == "__main__":
    sc = SparkContext("local[2]", "StreamingCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)  # 2 is a batchInterval prop of the DStream created by this StreamingContext

    ssc.checkpoint('file:///tmp/spark')

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))  # hostname and port

    def count_words(new_values, last_sum):
        if last_sum is None:
            last_sum = 0
        return sum(new_values, last_sum)

    word_counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .updateStateByKey(count_words)  # works across all entities in the stream

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()

# ncat -lk 9999
# spark-submit .\update_state_by_key.py localhost 9999
