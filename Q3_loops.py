import time
from pyspark import SparkContext
sc = SparkContext('local','app')


# Load the data from the input file
rdd_page = sc.textFile("pagecounts-20160101-000000_parsed.out")


# Define a function to normalize the page titles and extract the terms
def normalize_page_title(page_title):
    # Convert to lowercase and remove non-alphanumeric characters
    page_title = ''.join(e for e in page_title if e.isalnum() or e == '_').lower()
    # Extract the terms delimited by "_"
    terms = page_title.split("_")
    return terms


# Define variables for counting the unique terms
unique_term_count = 0
unique_terms = set()

start_time = time.time()

# Iterate over the RDD and count the number of unique terms
for line in rdd_page.collect():
    # Extract the page title
    page_title = line.split(" ")[1]
    # Normalize the page title and extract the terms
    terms = normalize_page_title(page_title)
    # Iterate over the terms and count the unique ones
    for term in terms:
        if term not in unique_terms:
            unique_term_count += 1
            unique_terms.add(term)
end_time = time.time()
elapsed_time = end_time - start_time


with open("Q3_loop.txt", "w") as f:
    f.write("Number of unique terms appearing in the page titles: {}\n".format(unique_term_count))

print("Elapsed time:", elapsed_time, "seconds")
