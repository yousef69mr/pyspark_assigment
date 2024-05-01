import time
from pyspark import SparkContext
# Initialize Spark context
sc = SparkContext("local", "Page_Size")

rdd_logs=sc.textFile("pagecounts-20160101-000000_parsed.out")

# Compute min, max, and average page size

page_size_min = float("inf")
page_size_max = float("-inf")
page_size_sum = 0
page_size_count = 0
start_time = time.time()

for data in rdd_logs.toLocalIterator():
    line = data.split(" ")

    line_field_size = int(line[3])
    if line_field_size < page_size_min:
        page_size_min = line_field_size
    if line_field_size > page_size_max:
        page_size_max = line_field_size
    page_size_sum += line_field_size
    page_size_count += 1
# make the avg_size to be mean
page_size_average = page_size_sum / page_size_count

end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")

with open("Q1_Loops.txt", "w") as f:
    f.write("Min page size: {}\n".format(page_size_min))
    f.write("Max page size: {}\n".format(page_size_max))
    f.write("Average page size: {}\n".format(page_size_average))
    f.write(f'Execution time :{elapsed_time}')
# Stop the Spark context
sc.stop()



