
from pyspark import SparkContext
import time
sc = SparkContext('local','app')

start_time = time.time()

rdd_page = sc.textFile("pagecounts-20160101-000000_parsed.out").map(lambda line: line.split(" ")[1])


# Define a function to normalize the page titles and extract the terms
def normalize_page_title(page_title):
    # Convert to lowercase and remove non-alphanumeric characters
    page_title = ''.join(e for e in page_title if e.isalnum() or e == '_').lower()
    # Extract the terms delimited by "_"
    terms = page_title.split("_")
    return terms


# Apply the normalization function to the RDD
rdd_terms = rdd_page.flatMap(normalize_page_title)

# Count the number of unique terms
num_unique_terms = rdd_terms.distinct().count()

# Save the results to an external file
with open("Q3_MAP.txt", "w") as f:
    f.write("Number of unique terms in page titles: {}\n".format(num_unique_terms))

# Stop the SparkContext
sc.stop()
end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")
