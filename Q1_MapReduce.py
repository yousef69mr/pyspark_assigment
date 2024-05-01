import time
from pyspark import SparkContext, SparkConf

# Create a SparkConf object
conf = SparkConf().setAppName("Q1.MapReduce").setMaster("local")


# Create a SparkContext object
sc = SparkContext(conf=conf)

# Read the data
data = sc.textFile("./pagecounts-20160101-000000_parsed.out")

# Extract the page size field and convert it to an integer
page_sizes = data.map(lambda line: int(line.split(" ")[3]))


# Compute the minimum, maximum, and average page size if the page_sizes RDD is not empty
if page_sizes.isEmpty():
    print("No data found")
else:
    start = time.time()
    min_size = page_sizes.reduce(lambda x, y: x if x < y else y)
    max_size = page_sizes.reduce(lambda x, y: x if x > y else y)
    #make the avg_size to be mean
    avg_size = page_sizes.reduce(lambda x, y: x + y) / page_sizes.count()
    end = time.time()
    # Print the time taken for the execution
    print("Time taken for the execution: ")
    print(end - start)
    # Print the results
    print("Minimum page size:", min_size)
    print("Maximum page size:", max_size)
    print("Average page size:", avg_size)

    # Save the results to a text file
    with open("Q1_MapReduce.txt", "w") as f:
        f.write("Minimum page size: " + str(min_size) + "\n")
        f.write("Maximum page size: " + str(max_size) + "\n")
        f.write("Average page size: " + str(avg_size) + "\n")
        f.write("Time taken for the execution: " + str(end - start) + " sec\n")

# Stop the SparkContext object
sc.stop()
