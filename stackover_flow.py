import re
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import mysql.connector
import os
from dotenv import load_dotenv
from time_period import Duration
from const import Constant

class ScrapingData:

    def __init__(self):
        load_dotenv()

        # Create a MySQL connection
        self.mydb = mysql.connector.connect(
            host = os.getenv('HOST_NAME'),
            user = os.getenv('USER_NAME'),
            password = os.getenv('PASSWORD'),
            database = os.getenv('DATABASE')
        )

    def separate_number(self, whole_string):
        match = re.search(r'\d+', whole_string)
        return int(match.group()) if match else 0

    def extract_data_from_page(self, url, data_list):
        driver = webdriver.Chrome()
        driver.get(url)

        for data in driver.find_elements(By.CLASS_NAME, "grid--item"):
            tag_name = data.find_element(By.CLASS_NAME, "post-tag").text
            days = data.find_element(By.CLASS_NAME, 'fc-black-400')
            no_of_questions = self.separate_number(days.find_element(By.TAG_NAME, 'div').text)

            rate_of_questions = data.find_element(By.CSS_SELECTOR, ".s-anchors.s-anchors__inherit")
            time_scope_of_question = rate_of_questions.find_elements(By.TAG_NAME, 'a')

            today, week, month, year = None, None, None, None

            for rate in time_scope_of_question:
                time_frame = rate.text.upper()
                count = self.separate_number(time_frame)
                if Duration.TODAY.name in time_frame:
                    today = count
                elif Duration.WEEK.name in time_frame:
                    week = count
                elif Duration.MONTH.name in time_frame:
                    month = count
                elif Duration.YEAR.name in time_frame:
                    year = count

            data_list.append({
                'tag_name': tag_name,
                'no_of_questions': no_of_questions,
                'today': today,
                'week': week,
                'month': month,
                'year': year,
                'scraped_time': datetime.datetime.now()
            })

        driver.quit()  # Close the WebDriver to release resources

    def run_scraping(self):
        # Initialize an empty list to store scraped data
        data_list = []
        main_url = Constant.STACK_OVERFLOW_URL

        # Loop through pages and extract data
        for page_num in range(1, 10):  # Adjust the range accordingly
            new_url = main_url.replace('page=1', f'page={page_num}')
            self.extract_data_from_page(new_url, data_list)

        # Create a cursor object to interact with the database
        cursor = self.mydb.cursor()

        # SQL insert query
        insert_query = (
            "INSERT INTO tech "
            "(tag_name, no_of_questions, today, week, month, year, scraped_time) "
            "VALUES "
            "(%(tag_name)s, %(no_of_questions)s, %(today)s, %(week)s, %(month)s, %(year)s, %(scraped_time)s)"
        )

        # Execute the insert query for each data record
        cursor.executemany(insert_query, data_list)

        # Commit the changes to the database
        self.mydb.commit()

        # Close the cursor and database connection
        cursor.close()
        self.mydb.close()

        # Print the extracted data
        for data in data_list:
            print(data)

# Create an instance of the ScrapingData class and run the scraping process
scraper = ScrapingData()
scraper.run_scraping()
