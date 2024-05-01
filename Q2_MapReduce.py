
from pyspark import SparkContext
import time
sc = SparkContext('local','app')

# Load the datafrom the input file
rdd_page = sc.textFile("pagecounts-20160101-000000_parsed.out")

#Map the page titles to a tuple of the form (project code, page title)

the_titles = rdd_page.map(lambda line: (line.split(" ")[0], line.split(" ")[1]))

# Filter the page titles that start with "The"

the_titles = the_titles.filter(lambda line: line[1].lower().startswith("the_"))

# Filter the "The" page titles that belong to the English project
en_titles_with_the = the_titles.filter(lambda line: line[0] != "en")

start = time.time()

count_the = the_titles.count()
# Count the number of "The" page titles that are not part of the English project
count_the_en = en_titles_with_the.count()
end = time.time()
# print in a file
print(end - start)

# Print the results in a file
with open("Q2_MapReduce.txt", "w") as f:
    f.write("Number of page titles that start with 'The': " + str(count_the) + "\n")
    f.write("Number of 'The' page titles not part of English project: " + str(count_the_en) + "\n")
    f.write("Time taken for the execution: " + str(end - start) + "\n")


# end_time = time.time()
# elapsed_time = end_time - start_time

# Print the results
print("Number of page titles that start with 'The':", count_the)
print("Number of 'The' page titles not part of English project:", count_the_en)


# Stop the Spark context
sc.stop()
