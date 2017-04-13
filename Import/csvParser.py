#!/usr/bin/env python

import csv

reader = csv.reader(open('file.csv', 'rb'))
writer = csv.writer(open('outfile.csv','wb'))
counter = 0;
for line in reader:	
    if 1 < counter: 
    	writer.writerow([counter, line[5], line[6], line[7], line[8]])
    counter+=1	
