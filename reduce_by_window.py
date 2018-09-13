import sys

from pyspark import SparkContext                # allow us to work with spark
from pyspark.streaming import StreamingContext  # allow to work with streams in spark

if __name__ == "__main__":
    sc = SparkContext("local[2]", "StreamingCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)  # 2 is a batchInterval prop of the DStream created by this StreamingContext

    ssc.checkpoint('file:///tmp/spark')

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))  # hostname and port

    summary = lines.reduceByWindow(
        lambda x, y: int(x) + int(y),  # sum function
        lambda x, y: int(x) - int(y),  # inverse func
        10, 2  # window size and sliding interval
    )

    summary.pprint()

    ssc.start()
    ssc.awaitTermination()

# ncat -lk 9999
# spark-submit .\reduce_by_window.py localhost 9999
