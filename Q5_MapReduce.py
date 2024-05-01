import time
from pyspark import SparkContext
sc = SparkContext('local','app')

start_time = time.time()
# Load the input data into an RDD
data = sc.textFile("pagecounts-20160101-000000_parsed.out")


# Function to parse each line and extract the necessary fields
def parse_line(line):
    fields = line.split(" ")
    return fields[1], fields[2:]

# Combine data of pages with the same title
def combine_page_data():
    title_data = data.map(parse_line)  # Map each line to (title, page_data) pairs
    combined_data = title_data.groupByKey().flatMap(
        lambda x: [(x[0], list(x[1]))])  # Group by title and flatten the values

    return combined_data

#for printing

# Combine page data using the map-reduce paradigm
combined_data = combine_page_data()
# print(combined_data.collect())
combined_data_list = combined_data.collect()
# print(combined_data_list[0])
end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")

# Write the output to a file
with open("Q5_MapReduce.txt", "w") as f:
    f.write(f'Time : {elapsed_time}\n')
    for title, data in combined_data_list:
        f.write(f"Title: {title}\n")
        for page_data in data:
            f.write(f"Page data: {page_data}\n")
        f.write("\n")

# Stop the SparkContext
sc.stop()

