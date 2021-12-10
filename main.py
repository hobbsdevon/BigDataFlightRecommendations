# Final Project 
# Group 1: Bram Dedrick, Devon Hobbs, Jakob Orel, Nathan Daniels

# This program will request the user for their desired flight information and will return travel information and 
# the best date to fly close to their desired trip.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Final Project: Group 1").getOrCreate()

def getAirportCodeFromCityState(airports_df, city, state):

    # Returns list of airports codes in specifie city, if none exist, returns empty list.
    
    return airports_df.filter(airports_df.CITY == city).filter(airports_df.STATE == state) \
            .select("IATA_CODE").rdd.map(lambda x: x[0]).collect()
            
def getAirlineCode(airlines_df, airline):

    # Returns list of airlines codes that contain the keyword.
    
    return airlines_df.filter(airlines_df.AIRLINE.contains(airline)).select('IATA_CODE').rdd\
            .map(lambda x: x[0]).collect()


def validDate(mmdd):

    # Checks to see if the date provided is a valid date.
    
    if int(mmdd[:2]) in [1, 3, 5, 7, 8, 10, 12]:
        return int(mmdd[2:]) in range(1, 32)
    elif int(mmdd[:2]) in [4, 6, 9, 11]:
        return int(mmdd[2:]) in range (1, 31)
    elif int(mmdd[:2]) == 2:
        return int(mmdd[2:]) in range(1, 29)
    return False	

def generateSurrounding(mmdd, size):

    # Generates a list of [month, day] pairs of 'size' in each direction around a specific date. 
    
    interval = []
    month = int(mmdd[:2])
    day = int(mmdd[2:])
    rollover = 30
    if month in [1, 3, 5, 7, 8, 10, 12]:
        rollover = 31
    elif month == 2:
        rollover = 28
        
    for increment in range(size + 1):
        interval.append([month, day])
        if day == rollover:
            day = 1
            if month == 12:
                month = 1
            else:
                month += 1
            
        else:
            day += 1
            
    month = int(mmdd[:2])
    day = int(mmdd[2:])
    rollover = 30
    if month in [2, 4, 6, 8, 9, 11, 1]:
        rollover = 31
    elif month == 3:
        rollover = 28
        
    for increment in range(size + 1):
        interval.append([month, day])
        if day == 1:
            day = rollover
            if month == 1:
                month = 12
            else:
                month -= 1
        else:
            day -= 1
            
    return interval
    
def generateInterval(mmdd0, mmdd1):

    # Generates a list of [month, day] pairs containing every day from one date to another.
    
    startMonth = int(mmdd0[:2])
    startDay = int(mmdd0[2:])
    endMonth = int(mmdd1[:2])
    endDay = int(mmdd1[2:])
    
    rollover = 30
    if startMonth in [1, 3, 5, 7, 8, 10, 12]:
        rollover = 31
    elif startMonth == 2:
        rollover = 28
        
    interval = []
    
    if startMonth == endMonth and startDay == endDay:
        interval.append([startMonth, startDay])
        
    while not (startMonth == endMonth and startDay == endDay):
        interval.append([startMonth, startDay])
        if startDay == rollover:
            if startMonth == 12:
                startMonth = 1
            else:
                startMonth += 1
            startDay = 1
            rollover = 30
            if startMonth in [1, 3, 5, 7, 8, 10, 12]:
                rollover = 31
            elif startMonth == 2:
                rollover = 28
        else:
            startDay += 1
    return interval
    
def checkAirportCodes(airports_df, airport_codes):

    # Checks to make sure an airport code is containted within the list of airports.
    
    for airport_code in airport_codes:
        if airports_df.filter(F.col("IATA_CODE").isin(airport_code)).select("IATA_CODE").rdd.isEmpty():
            return False
    return True

def checkAirlineCodes(airlines_df, airline_codes):
    
    # Checks to make sure an airlines code is containted within the list of airlines.
    
    for airline_code in airline_codes:
        if airlines_df.filter(F.col("IATA_CODE").isin(airline_code)).select("IATA_CODE").rdd.isEmpty():
            return False
    return True

def getInputs(airports_df, airlines_df):

    # This function will generate all the needed inputs from the user.
    
    print("\n", "=".center(100, "="), "\n")
    print("\033[1mCSC 355 Group 1's Flight Recommendation Program\033[0m".center(100), "\n")
    print("~ This program will make recommendations of when to fly to avoid delays based off flight information from 2015.")
    print("~ Below you will be able to make filter your list.")
    print("~ Any field left black will be interpreted as you wish to use all available data.")
    print("\033[3m~ I.e. if you do not specify an airline when prompted, all airlines will be included in your search.\033[0m")

    
    print("\n", "=".center(100, "="), "\n")
    
    # Gets a list of all departure airport codes that the user specifies.
    
    while True:
        print("\033[1mFrom where would you like to depart?\033[0m".center(100), "\n")
                            
        departures = input("~ You may specify more than one using commas.\n"
                            "\033[3m~ E.g.: ORD, MWD \033[0m \n"
                            "~ If you want to seach by city type: CITY \n").upper()
        if departures == "CITY":
            city = (input("~ Please specify city (\033[3me.g. BUFFALO\033[0m): ")).upper()
            state = (input("~ Please specify state (\033[3me.g. NY\033[0m): ")).upper()
            departures = getAirportCodeFromCityState(airports_df, city, state)
            if not departures:
                print("\033[1mCity not found, please try again.\033[0m \n")
                continue
            else:
                break
        
        if departures == "":
            departures = []
            break
            
        departures = departures.replace(" ", "")
        departures = departures.split(",")
        if checkAirportCodes(airports_df, departures): 
            break
        print("\033[1mCannot find specified airport(s), please try again.\033[0m \n")
        
    print("\n", "=".center(100, "="), "\n")
    
    # Gets list of all arrival airport codes that the user specifies. 
    
    while True:
        print("\033[1mWhere would you like to arrive?\033[0m".center(100), "\n")
        
        arrivals = input("~ You may specify more than one using commas. \n"
                        "\033[3m~ E.g.: ORD, MWD\033[0m \n"
                        "~ If you want to seach by city type: CITY \n").upper()
        if arrivals == "CITY":
            city = (input("~ Please specify city (\033[3me.g. BUFFALO\033[0m): ")).upper()
            state = (input("~ Please specify state (\033[3me.g. NY\033[0m): ")).upper()
            arrivals = getAirportCodeFromCityState(airports_df, city, state)
            if not arrivals:
                print("\033[1mCity not found, please try again.\033[0m \n")
                continue
            else:
                break
        if arrivals == "":
            arrivals = []
            break
        arrivals = arrivals.replace(" ", "")
        arrivals = arrivals.split(",")
        if checkAirportCodes(airports_df, arrivals):
            break
        print("\033[1mCannot find specified airport(s), please try again.\033[0m \n")
        
    print("\n", "=".center(100, "="), "\n")
    
    # Gets a list of all airline codes that the user specifies.
    
    while True:
        print("\033[1mWhich airline(s) would you like to use?\033[0m".center(100), "\n")
                        
        airlines = input("~ You may specify more than one using commas. \n"
                        "\033[3m~ E.g.: AA, WN \033[0m \n"
                        "~ To specify an airline by name, type: NAME \n").upper()
        if airlines == "NAME":
            airline = input("~ Enter airline name: ").upper()
            airlines = getAirlineCode(airlines_df, airline)
            if not airlines:
                print("\033[1mNo airline found, please try again.\033[0m \n")
                continue
            else:
                break
        if airlines == "":
            airlines = []
            break
        airlines = airlines.replace(" ", "")
        airlines = airlines.split(",")
        if checkAirlineCodes(airlines_df, airlines):
            break
        print("\033[1mCannot find specified airline(s), please try again.\033[0m \n")
        
    print("\n", "=".center(100, "="), "\n")
    
    # Gets the interval of when a user would like to fly.

    while True:
        print("\033[1mWhen would you like to fly?\033[0m".center(100), "\n")
                      
        dates = input("~ Specify one date to search around that time, specify two, separated by commas, to define an interval. \n"
                        "~ Use \"MMDD\" notation. \033[3mE.g. September 15th: 0915\033[0m \n")
        if dates == "":
            dates = []
            break
        if len(dates) == 4:
            if validDate(dates):
                dates = generateSurrounding(dates, 10)
                break
        
        dates = dates.replace(" ", "")
        if len(dates) == 9:
            dates = dates.split(",")
            if validDate(dates[0]) and validDate(dates[1]):
                dates = generateInterval(dates[0], dates[1])
                break
        print("\033[1mInvalid date(s), please try again.\033[0m \n")
        
    return departures, arrivals, airlines, dates
    

def readDataFromCSV():

    # Generates all needed dataframes from the CSV files.
    
	flights_df = spark.read.options(header='True', inferschema='True').csv("flights.csv")
	airlines_df = spark.read.options(header='True', infershema='True').csv("airlines.csv")
	airports_df = spark.read.options(header='True', inferschema='True').csv("airports.csv")
	for col in airlines_df.columns:
	    airlines_df = airlines_df.withColumn(col, F.upper(F.col(col)))
	for col in airports_df.columns:
	    airports_df = airports_df.withColumn(col, F.upper(F.col(col)))
	return flights_df, airlines_df, airports_df

# Filter dataframes based on input
def filterFlights(flights_df, departure_input, arrival_input, airline_input, date_input):

    # Filters the entire flights dataframe by what the user specified.
    
    filtered_flights = flights_df
    
    # Filter departure airports
    if departure_input:
        filtered_flights = filtered_flights.filter(F.col("ORIGIN_AIRPORT").isin(departure_input)).select("*")
    
    # Filter arrival airports
    if arrival_input:
        filtered_flights = filtered_flights.filter(F.col("DESTINATION_AIRPORT").isin(arrival_input)).select("*")
    
    # Filter airlines
    if airline_input:
        filtered_flights = filtered_flights.filter(F.col("AIRLINE").isin(airline_input)).select("*")
    	
    # Filter for date interval
    # Get month day lists
    # This process avoids looping through all of the dates and creating new dataframes and joining them at the end
    # Otherwise we would have to look at every month, date pair and add it to a new dataframe
    if date_input:
        month_days = [[] for i in range(12)]
	
        for date in date_input:
            month_days[date[0]-1].append(date[1])
	
        filtered_flights = filtered_flights.filter(((filtered_flights.MONTH==1) & (filtered_flights.DAY.isin(month_days[0]))) | \
                                                    ((filtered_flights.MONTH==2) & (filtered_flights.DAY.isin(month_days[1]))) | \
                                                    ((filtered_flights.MONTH==3) & (filtered_flights.DAY.isin(month_days[2]))) | \
                                                    ((filtered_flights.MONTH==4) & (filtered_flights.DAY.isin(month_days[3]))) | \
                                                    ((filtered_flights.MONTH==5) & (filtered_flights.DAY.isin(month_days[4]))) | \
                                                    ((filtered_flights.MONTH==6) & (filtered_flights.DAY.isin(month_days[5]))) | \
                                                    ((filtered_flights.MONTH==7) & (filtered_flights.DAY.isin(month_days[6]))) | \
                                                    ((filtered_flights.MONTH==8) & (filtered_flights.DAY.isin(month_days[7]))) | \
                                                    ((filtered_flights.MONTH==9) & (filtered_flights.DAY.isin(month_days[8]))) | \
                                                    ((filtered_flights.MONTH==10) & (filtered_flights.DAY.isin(month_days[9]))) | \
                                                    ((filtered_flights.MONTH==11) & (filtered_flights.DAY.isin(month_days[10]))) | \
                                                    ((filtered_flights.MONTH==12) & (filtered_flights.DAY.isin(month_days[11]))))
	
    return filtered_flights

def findMonthLength(month):

    # Returns the length of a given month
    
    month_end = 30
    if month in [1,3,5,7,8,10,12]:
        month_end = 31
    elif month == 2:
        month_end = 28
    return month_end
    
def distanceFromDay(day1, day2):

    # Returns distance from day1 to day2
    
    if day1[0] == day2[0]:
        return abs(day1[1]-day2[1])  
    elif (day1[0] > day2[0]) or (day1[0] == 1 and day2[0] == 12):
        return day1[1] + findMonthLength(day2[0])-day2[1]
    elif day1[0] < day2[0]:
        return findMonthLength(day1[0]) - day1[1] + day2[1]

def findBestDay(flights,days):

    # Returns the best day for someone to fly over the interval 
    
    flights = flights.fillna(0)
    
    if days and days.count(days[0]) == 2:
        flights = flights.withColumn('WEIGHTED_SCORE',4000 if flights.select('CANCELLED').collect()[0].CANCELLED == '1' else\
            (((flights.DEPARTURE_DELAY + 50)*(1+0.3*distanceFromDay(days[0],\
            [int(flights.select('MONTH').collect()[0].MONTH),int(flights.select('DAY').collect()[0].DAY)]))-50) +\
            flights.WEATHER_DELAY*1.3 + flights.LATE_AIRCRAFT_DELAY*0.3 + flights.AIRLINE_DELAY*0.7 +\
            flights.SECURITY_DELAY*0.55 +flights.AIR_SYSTEM_DELAY*0.95))             
    else:
        flights = flights.withColumn('WEIGHTED_SCORE',4000 if flights.select('CANCELLED').collect()[0].CANCELLED == '1' else\
            flights.DEPARTURE_DELAY +(flights.WEATHER_DELAY*1.3 +\
            flights.LATE_AIRCRAFT_DELAY*0.3 + flights.AIRLINE_DELAY*0.7 + flights.SECURITY_DELAY*0.55 + flights.AIR_SYSTEM_DELAY*0.95))
            
    averages = flights.groupBy('MONTH','DAY').avg('WEIGHTED_SCORE')
    minimum = averages.agg({'avg(WEIGHTED_SCORE)':'min'}).withColumnRenamed('min(avg(WEIGHTED_SCORE))','MINIMUM')
    
    return averages.filter(F.col('avg(WEIGHTED_SCORE)') == minimum.collect()[0].MINIMUM).drop("avg(WEIGHTED_SCORE)")
    
def main():
    
    # Main function that will call all needed functions.

	flights_df, airlines_df, airports_df = readDataFromCSV()

	departure_input, arrival_input, airline_input, date_input = getInputs(airports_df, airlines_df)	
	
	filtered_flights = filterFlights(flights_df,departure_input, arrival_input, airline_input, date_input)
	
	if filtered_flights.rdd.isEmpty():
	    print( "\033[1mNo flight recommendations based on given input.\033[0m".center(100))
	else:
	    print("\n", "=".center(100, "="), "\n")
	    print("\033[1mThe best day to fly is!\033[0m".center(100), "\n")
	    findBestDay(filtered_flights,date_input).show()

if __name__=="__main__":
	main()
