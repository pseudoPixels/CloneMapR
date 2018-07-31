import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd












potential_clones = 'Datasource/pc.xml'












def convertAndSaveAsCSV(inputPath, destinationPath):
	soup = ''
	with open(inputPath) as fp:
		soup = BeautifulSoup(fp, 'lxml')

	all_potential_clones = soup.find_all('source')


	total_pcs = len(all_potential_clones)


	df = pd.DataFrame(columns=["filepath", "startline", "endline", "sourceCode"])


	total_pcs = len(all_potential_clones)

	for i in range(0, total_pcs):
		src = all_potential_clones[i].text
		src = src.replace('\n', ' ').replace('\r', '')
	
		df = df.append({'filepath': all_potential_clones[i]['file'], 'startline': all_potential_clones[i]['startline'], 'endline': all_potential_clones[i]['endline'], 'sourceCode': src}, ignore_index=True)


	df.to_csv(destinationPath, sep=',')




convertAndSaveAsCSV(potential_clones,'csvCodes.csv')
