#import libraries
from bs4 import BeautifulSoup, SoupStrainer
from polyfuzz import PolyFuzz
import concurrent.futures
import csv
import pandas as pd
import requests

#import urls
with open("source_urls.txt", "r") as file:
    url_list_a = [line.strip() for line in file]

with open("target_urls.txt", "r") as file:
    url_list_b = [line.strip() for line in file]

#create a content scraper via bs4
def get_content(url_argument):
    page_source = requests.get(url_argument).text
    strainer = SoupStrainer('p')
    soup = BeautifulSoup(page_source, 'lxml', parse_only=strainer)
    paragraph_list = [element.text for element in soup.find_all(strainer)]
    content = " ".join(paragraph_list)
    return content

#scrape the urls for content
with concurrent.futures.ThreadPoolExecutor() as executor:
    content_list_a = list(executor.map(get_content, url_list_a))
    content_list_b = list(executor.map(get_content, url_list_b))

content_dictionary = dict(zip(url_list_b, content_list_b))

#get content similarities via polyfuzz
model = PolyFuzz("TF-IDF")
model.match(content_list_a, content_list_b)
data = model.get_matches()

#map similarity data back to urls
def get_key(argument):
    for key, value in content_dictionary.items():
        if argument == value:
            return key
    return key
	
with concurrent.futures.ThreadPoolExecutor() as executor:
    result = list(executor.map(get_key, data["To"]))

#create a dataframe for the final results
to_zip = list(zip(url_list_a, result, data["Similarity"]))
df = pd.DataFrame(to_zip)
df.columns = ["From URL", "To URL", "% Identical"]

#export to a spreadsheet
with open("redirect_map.csv", "w", newline="") as file:
    columns = ["From URL", "To URL", "% Identical"]
    writer = csv.writer(file)
    writer.writerow(columns)
    for row in to_zip:
        writer.writerow(row)
