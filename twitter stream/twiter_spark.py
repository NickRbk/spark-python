import sys

from pyspark import SparkContext                # allow us to work with spark
from pyspark.streaming import StreamingContext  # allow to work with streams in spark


def create_context(host, port):
    print "Creating new context"
    sc = SparkContext("local[2]", "StreamingCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)  # 2 is a batchInterval prop of the DStream created by this StreamingContext

    ssc.checkpoint('file:///tmp/spark')

    lines = ssc.socketTextStream(host, port)  # hostname and port

    def count_words(new_values, last_sum):
        if last_sum is None:
            last_sum = 0
        return sum(new_values, last_sum)

    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda w: w.startswith("#")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(count_words)

    word_counts.pprint()
    return ssc


if __name__ == "__main__":

    host, port, checkpoint_dir = sys.argv[1:]
    print checkpoint_dir
    ssc = StreamingContext.getOrCreate(checkpoint_dir,
                                       lambda: create_context(host, int(port)))

    ssc.start()
    ssc.awaitTermination()


# python twitter_stream.py
# spark-submit .\twiter_spark.py localhost 7777 file:///tmpspark-streaming-twitter
