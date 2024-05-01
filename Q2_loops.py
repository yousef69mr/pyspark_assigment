import time
from pyspark import SparkContext

sc = SparkContext('local','app')

# Load the datafrom the input file
rdd_page = sc.textFile("pagecounts-20160101-000000_parsed.out")

# Define variables for counting the page titles
the_title_count = 0
non_en_title_count = 0
start_time = time.time()
# Iterate over the RDD and count the page titles that start with "The" and the ones that start with "The" but are not part of the English project
for line in rdd_page.collect():
    # Extract the page title and project code
    splits = line.split(" ")
    page_title = splits[1]
    project_code = splits[0]

    # Check if the page title starts with "The"
    if page_title.lower().startswith("the"):
        the_title_count += 1

        # Check if the project code is not "en"
        if not project_code.startswith("en"):
            non_en_title_count += 1

end_time = time.time()
elapsed_time = end_time - start_time

print("Elapsed time:", elapsed_time, "seconds")


# Save the results to an external file
with open("Q2_loop.txt", "w") as f:
    f.write("Number of page titles that start with 'The': {}\n".format(the_title_count))
    f.write("Number of page titles that start with 'The' but are not part of the English project: {}\n".format(
        non_en_title_count))
sc.stop()
