import requests
import csv
import json
import re
import bs4
import threading
import os

from bs4 import BeautifulSoup

csv_lock = threading.Lock()

DATA_FILE_NAME = 'data.csv'
DATA_FILE_ENCODING = 'utf-16'

def extract_name(raw_tag : bs4.Tag):
    full_text = list(raw_tag.children)[0].text
    return full_text[:full_text.find("-") - 1]

def extract_price(raw_tag : bs4.Tag):
    return raw_tag.text[:-1]

def extract_location(raw_tag : bs4.Tag):
    filtered_location = raw_tag.text[3:]
    
    division = filtered_location.find(",")
    if (division > 0):
        subcity = filtered_location[:division]
        city = filtered_location[division + 2:]
        return (subcity, city)
    else:
        return ("None", filtered_location)

def extract_tag(raw_tag : bs4.Tag):
    cleared_tags = raw_tag.select(":nth-child(odd)")
    if len(cleared_tags) > 0:
        total_tag : str = cleared_tags[0].text
        for filtered_tag_index in range(1, len(cleared_tags)):
            filtered_tag = cleared_tags[filtered_tag_index]
            total_tag += f";{filtered_tag.text}"
        return total_tag
    else:
        return list(raw_tag.children)[0].text

def extract_profile_url(raw_tag : bs4.Tag):
    return raw_tag.get("href")

def extraft_teacher_id(raw_tag : bs4.Tag):
    return list(list(raw_tag[0].children)[0].children)[0].get("href")[len('/in/'):]

def scrapping(work_type : str, url : str):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        print(url)

        names = soup.select('.name')
        prices = soup.select('.price')
        locations = soup.select('.user-location')
        results_tags = soup.select('.result-tags')

        for raw_name, raw_price, raw_location, raw_tag in zip(names, prices, locations, results_tags):
            name = extract_name(raw_name)
            price = extract_price(raw_price)
            subcity, city = extract_location(raw_location)
            tag = extract_tag(raw_tag)

            teacher_id = "None"
            response = requests.get(f"https://www.apprentus.be{extract_profile_url(raw_name)}")
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                teachers_ids = soup.select('.profilename')
                teacher_id = extraft_teacher_id(teachers_ids)

            teacher_data = (name, work_type, teacher_id, price, subcity, city, tag)
            
            with csv_lock:
                with open(DATA_FILE_NAME, 'a', newline='', encoding=DATA_FILE_ENCODING) as file:
                    writer = csv.writer(file)
                    writer.writerow(teacher_data)

def launching(work_type : str, thread_chunk : int, url_func):    
    response = requests.get(url_func(0))
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        teacher_count : int = int(re.findall(r'\d+', soup.select('.teacher-count')[0].text)[0]) // 25

        for teacher_index in range(0, teacher_count, thread_chunk):
            threads = []
            for thread_index in range(thread_chunk):
                thread = threading.Thread(target=scrapping, args=(work_type, url_func(teacher_index + thread_index),))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

if not os.path.exists(os.path.abspath(DATA_FILE_NAME)):
    with open(DATA_FILE_NAME, 'w', newline='', encoding=DATA_FILE_ENCODING) as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'work_type', 'teacher_id', 'price', 'subcity', 'city', 'tags'])

launching("soutien-scolaire", 5, lambda index : f"https://www.apprentus.be/soutien-scolaire/belgique/{index}")
#launching("en-ligne", 5, lambda index : f"https://www.apprentus.be/cours-particuliers/en-ligne/{index}")