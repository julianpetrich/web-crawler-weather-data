from selenium import webdriver
from selenium.webdriver.support.ui import Select
import os
import pandas as pd
import numpy as np
import re


class WebScraper():
    """Scrapes the temperature and wind speed data from website for
        specific weather stations and writes the data into required JSON
        file.
    
    :param station_dict: Dictionary that defines with stations to be scraped
        and which data should be scraped for each corresponding station.
        
    :ivar data_temp: DataFrame for temperature data in the required format
        ['Time', 'Value', 'Station'] in which Time represents the first day
        of the month that was scraped, Value represents the average 
        temperature for that month, and Station represents the name of the 
        weather station
    :ivar data_wind: DataFrame for wind speed data in the required format
        ['Time', 'Value', 'Station'] in which Time represents the first day
        of the month that was scraped, Value represents the average 
        wind speed for that month, and Station represents the name of the 
        weather station
    """

    def __init__(self, station_dict):
        
        self.station_dict = station_dict
        
        self.data_temp = pd.DataFrame(columns=['Time', 'Value', 'Station'])
        self.data_wind = pd.DataFrame(columns=['Time', 'Value', 'Station'])
            
        self.pipeline()
            
    def pipeline(self):
        """Starts the pipeline for the webscraper:
            
            1. Setting up the webdriver
            2. Scraping the data from the webpage(s)
            3. Writing the data into the required JSON format
        """
        
        self.set_webdriver()
        self.get_data()
        self.driver.close()
        
        self.write_data()
                
    def set_webdriver(self):
        """Sets up the webscraper:
            
            1. Sets the download folder to be same path
            2. Sets Chromium to be used in headless mode
            3. Sets executable path for Chromium
        """ 
        
        options = webdriver.ChromeOptions()
        
        # Sets download path
        dir_current = os.getcwd()
        prefs = {'download.default_directory': dir_current}
        options.add_experimental_option('prefs', prefs)
        
        # Sets headless mode
        options.add_argument('--headless')
        options.add_argument('--window-size=1920x1080')
        
        # Sets executable path for Chromium Driver
        exec_path = '/usr/local/bin/chromedriver'
        
        self.driver = webdriver.Chrome(options=options,
                                       executable_path=exec_path)
    
    def load_data(self, driver, station, data_type):
        """Scrapes the required data from the webpage. Loads the data into
        respective DataFrame (temperature or wind speed).
            
        :param driver: Webdriver that is used for scraping
        :param station: Weather station that will be scraped
        :data_type: Sets which atmospheric data will be scraped 
            (1 for temperature; 4 for wind speed)
        """
        
        table_xpath = '//*[contains(@class,"data2_s")]//tr'
        elem_xpath = './/*[self::td or self::th]'
        
        # Finds table containing the values and selects and selects data
        # from top to bottom and left to right
        for tab in driver.find_elements_by_xpath(table_xpath):
            data = [i.text for i in tab.find_elements_by_xpath(elem_xpath)]
            
            year = data[0]
            
            # Only scrapes data if the row is not the header row
            if year != 'Year':
                
                print('... Scraping year: {}'.format(year))
                
                # Reshapes values array from row into column
                values = np.array(data[1:-1])
                values = values.reshape(-1, 1)
                
                # Sets timestamp for each entry to first of the month
                dates = [pd.to_datetime(str(year)+'-'+str(m)+'-1').strftime(
                         format='%Y-%m-%d') for m in range(1, 13, 1)]

                data_insert = pd.DataFrame(data=values)
                data_insert.columns = ['Value']
                data_insert['Time'] = dates
                data_insert['Station'] = station
                
                # Makes sure that the values represent valid floats
                data_insert.Value = data_insert.Value.apply(lambda row: re.sub(
                        r'\nRevision: (\d+\.+\-)', '', row))
                                
                if data_type == '1':

                    self.data_temp = pd.concat([self.data_temp, data_insert],
                                               axis=0,
                                               ignore_index=True,
                                               sort=False)

                elif data_type == '4':

                    self.data_wind = pd.concat([self.data_wind, data_insert],
                                               axis=0,
                                               ignore_index=True,
                                               sort=False)
        
    def get_data(self):
        """Initializes the scraping of the stations' data. Defines with 
            stations will be scraped with their respective channel 
            (temperature or wind speed).
        """
        
        # The main page to be scraped
        url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/monthly_s3_en.php?block_no=47401&view=1'
        
        # Opens the URL
        self.driver.get(url)
        self.driver.implicitly_wait(10)
        
        for station in self.station_dict:
            
            print('Starting job for {}'.format(station))
            
            # Selects the station from the drop down menu
            sel_station = Select(self.driver.find_element_by_name('block_no'))
            sel_station.select_by_value(station_dict[station][0])
            
            # Selects the data type that must be scraped for the station
            el_xpath = '//option[@value="{}"]'.format(station_dict[station][1])
            self.driver.find_element_by_xpath(el_xpath).click()
            
            # Submits selection
            sub_xpath = '//input[@value="Refresh"]'
            self.driver.find_element_by_xpath(sub_xpath).click()
            
            self.driver.implicitly_wait(10)
            
            # Starts extraction of the data
            self.load_data(self.driver, station, station_dict[station][1])
            
            self.driver.implicitly_wait(10)
            
            print('Finished job for {}'.format(station))
            print('\n')

    def write_data(self):
        """Writes the scraped data into two JSON files in the local directory.
        """
            
        self.data_temp.to_json(orient='records', path_or_buf='data_temp.json')
        self.data_wind.to_json(orient='records', path_or_buf='data_wind.json')
        

# The stations to be scraped, first entry is the ID used on the 
# website for each station, second entry is the ID used for data type
# (1: temperature, 2: wind speed)
station_dict = {
    'WAKKANAI': ['47401', '1'],
    'HABORO': ['47404', '1'],
    'RUMOI': ['47406', '1'],
    'OBIHIRO': ['47417', '1'],
    'OMU': ['47405', '4'],
    'SUTTSU': ['47421', '4'],
    'MURORAN': ['47423', '4'],
    'KUTCHAN': ['47433', '4'],
}

bot = WebScraper(station_dict)
