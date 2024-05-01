import time
from pyspark import SparkContext
sc = SparkContext('local','app')

data = sc.textFile("pagecounts-20160101-000000_parsed.out")


def parse_line(line):
    fields = line.split(" ")
    return fields[1]


def extract_title_counts():
    title_counts = {}
    for line in data.toLocalIterator():
        title = parse_line(line)
        if title in title_counts:
            title_counts[title] += 1
        else:
            title_counts[title] = 1

    return title_counts

start_time = time.time()
title_counts = extract_title_counts()

end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")

# Write the output to a file
with open("Q4_loops.txt", "w") as f:
    f.write(f'Time : {elapsed_time}\n')
    for title, count in title_counts.items():
        f.write(f"{title}: {count}\n")
sc.stop()