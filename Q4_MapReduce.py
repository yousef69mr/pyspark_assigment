import time
from pyspark import SparkContext
sc = SparkContext('local','app')

start_time = time.time()

input_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")

# map each title to a count of 1
title_counts_rdd = input_rdd.map(lambda line: (line.split(" ")[1], 1))

# reduce by key to get the count of each title
title_count_rdd = title_counts_rdd.reduceByKey(lambda a, b: a + b)
count = title_count_rdd.count()
print("The number of tuples are : " +str(count))
end = time.time()

end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")


# Write the output to a file
with open("Q4_map.txt", "w") as f:
    f.write(f'Execution time :{elapsed_time}')
    f.write(f'Count : {count}')

    # for title, count in title_count_rdd.items():
    #     f.write(f'{title}: {count}\n')

# title_count_rdd.coalesce(1).saveAsTextFile("Q4_map")
# stop SparkContext
sc.stop()
