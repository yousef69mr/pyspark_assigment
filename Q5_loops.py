
import time
from pyspark import SparkContext
sc = SparkContext('local','app')


start_time = time.time()

data = sc.textFile("pagecounts-20160101-000000_parsed.out")


def parse_line(line):
    fields = line.split(" ")
    return fields[1], fields[2:]


# Combine data of pages with the same title
def combine_page_data():
    title_data = {}

    for line in data.toLocalIterator():
        title, page_data = parse_line(line)

        if title in title_data:
            title_data[title].append(page_data)
        else:
            title_data[title] = [page_data]

    combined_data = [(title, data) for title, data in title_data.items()]

    return combined_data


# Combine page data using Spark loops
combined_data = combine_page_data()

end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")
# Write the output to a file
with open("Q5_loops.txt", "w") as f:
    f.write(f'Time : {elapsed_time}\n')
    for title, data in combined_data:
        f.write(f"Title: {title}\n")
        for page_data in data:
            f.write(f"Page data: {page_data}\n")
        f.write("\n")

# Stop the SparkContext
sc.stop()
