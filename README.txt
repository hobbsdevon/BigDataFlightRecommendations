Flight Recommendations based on 2015 Data
Final Project for CSC355 Intro to Big Data Frameworks




Group 1: Bram Dedrick, Devon Hobbs, Jakob Orel, Nathan Daniels


This program is designed to take input from a user via the command line about the trip they would like to take. The program will then return the most desirable date to travel closest to their desired date(s) and parameters. It also gives suggestions for best airline and destinations for the most enjoyable flight experience.


A description of user input is as follows:
Departure- The user may enter the airport code, or city and state they would wish to travel from. The program will match the input to the airport code(s) of the dataset that match their input. If multiple departure airport criteria are given, multiple airport codes will be selected in the dataset. If no input is given, the program will assume the user is open to travel from any airport. If an invalid input is given, the program will return a prompt to enter a valid input.
Arrival- The user may enter the airport code, or city and state they would wish to travel to. The program will match the input to the airport code(s) of the dataset that match their input. If multiple arrival airport criteria are given separated by commas, multiple airport codes will be selected in the dataset. If no input is given, the program will assume the user is open to travel to any airport. If an invalid input is given, the program will return a prompt to enter a valid input.
Airline- The user may provide the airline code or airline name that they wish to travel with. They may choose multiple or none.
Date- The user may enter a date(s) (MMDD) they wish to travel on. If they enter a single date, the program will find the best dates to travel 10 days before and after the given date. The user may also enter two dates separated by a comma to give a date interval. The program would then return suggestions for flying that may be most desirable.


In the case of arrival and departure inputs, should the user input city and state, rather than airport code, that will be converted into airport code, and city/state will no longer be used. Likewise, if the user chooses to input the name of the airline they intend to use, that will be converted to the airline code, and the actual name of the airline will no longer be used.


The output of the program will be the best day to fly that best fits the user’s inputs.