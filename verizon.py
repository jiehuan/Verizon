from selenium import webdriver
from selenium.common.exceptions import WebDriverException
import time
import csv
from datetime import datetime
import os
import pandas as pd
import numpy as np

import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import re

def crawler(update=False, date_last, time_last):
    url = 'https://www.verizonwireless.com/smartphones/apple-iphone-x/'

    browser = webdriver.Chrome(executable_path='./chromedriver')
    browser.get(url)
    time.sleep(3)

    review_c = browser.find_element_by_xpath('//*[@id="reviewsLink"]')
    review_c.location_once_scrolled_into_view
    time.sleep(0.5)
    review_c.click()
    time.sleep(2)

    drop_c = browser.find_element_by_xpath('//*[@id="reviews"]/div/div[1]/div/div[3]/div[2]/div[2]/div/div/span[2]/span')
    drop_c.location_once_scrolled_into_view
    time.sleep(0.5)
    drop_c.click()
    browser.find_element_by_xpath("//*[text()='Newest']").click()
    time.sleep(2)

    reviews = []
    titles = []
    users = []
    dates = []

    review_section = '//*[@id="reviews"]/div/div[2]/div/div[*]/div[1]/div/div[1]/div[3]'
    title_section = '//*[@id="reviews"]/div/div[2]/div/div[*]/div[1]/div/div[1]/div[2]'
    userid_section = '//*[@id="reviews"]/div/div[2]/div/div[*]/div[1]/div/div[1]/div[1]/div[2]/span[1]'
    date_section = '//*[@id="reviews"]/div/div[2]/div/div[*]/div[1]/div/div[1]/div[1]/div[2]/meta[1]'
    
    if update = False:
        date1 = '//*[@id="reviews"]/div/div[2]/div/div[1]/div[1]/div/div[1]/div[1]/div[2]/meta[1]'
        date_now = browser.find_elements_by_xpath(date1)[0].get_attribute("content")
        time_now = browser.find_elements_by_xpath(date1)[0].get_attribute("content")
    
    while True:
        a = browser.find_elements_by_xpath(review_section)
        reviews += [each.text for each in a]

        b = browser.find_elements_by_xpath(title_section)
        titles += [each.text for each in b]

        c = browser.find_elements_by_xpath(userid_section)
        users += [each.text for each in c]

        d = browser.find_elements_by_xpath(date_section)
        dates += [(each.get_attribute("content")[:10]) for each in d]

        try:
            next = browser.find_element_by_xpath('//*[@id="reviews"]/div/div[3]/div/ul/li[6]/a/span[1]')
            next.location_once_scrolled_into_view
            time.sleep(0.5)  # To wait until scrolled down to "Next" button
            next.click()
            time.sleep(2)  # To wait for page "autoscrolling" to first review + until modal window dissapeared
        
        except WebDriverException:
            break

    browser.quit()

    devices = ['Samsung Galaxy s7'] * len(users)

    pfile = pd.DataFrame(np.array([devices, titles, reviews, dates, users]).T, columns = ['Device', 'Title', 'ReviewText', 'SubmissionTime', 'UserNickname'])

    filename = "Verison_" + str(datetime.today())[:13].replace(" ", "-") + ".csv"
    
    pfile.to_csv(filename, index = False)

return filename
    
def work_count(filename):
    conf = SparkConf().setAppName("Verizon_samsung-galaxy-s7: Reviews Data Analysis")
    sc = SparkContext(conf=conf)

    savepath = './' + str(datetime.today())[:10] + '_' + str(datetime.today())[12:19]

    try:
        file = sc.textFile(filename).cache()
        file.take(1)  # to test if file exists
        file_Available = "Yes"
    except:
        file_Available = "No"
        print("File Not Available {}".format(filename))

    if file_Available == "Yes":
        pattern = re.compile('[^A-Za-z0-9\' ]')
        wordList = file.map(lambda row: row.split(",")[2]).flatMap(lambda row: pattern.sub(' ',row.lower()).split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda row: row[1], ascending=False).map(lambda row: row[0] + "," + str(row[1])).take(250)
        sc.parallelize(wordList).coalesce(1).saveAsTextFile(savepath)
    else:
        sc.stop()
        
def main():
    while True:
        file = crawler()
        work_count(file)
        time.sleep(3600)
