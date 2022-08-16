#!/usr/bin/python
import re
import math
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

#outline what we doing: step 1 get from file, then find frequency, then num years for word in file, then calc

class TFID(MRJob):
	SORT_VALUES = True
	
	#takes input, splits into date and word where date is only the year
	#yield the year, word and 1 -> 1 instance of this word in this year
	def mapper_pairs(self, _, line):
		words = re.split("[,]", line.lower())
		
               	#split date up for only year 
		date = re.findall(r'^\d{4}', words[0])
		
		word = re.split("[ ]", words[1])
		
		for w in word:
			yield ((date[0], w), 1)
	
	
	#groups all same year words counts together
	def reducer_1(self, pairs, count):
		x,y = pairs
		yield y, (x, sum(count))   
	
	#attain freq and number of words values
	def terms(self, word, frequency):
		num = 0
		terms = []
		freqs = []
		for termFreq in frequency:
			term = termFreq[0]
			freq = termFreq[1]
			terms.append(term)
			freqs.append(freq)
			num += 1
		
		#yield (year, word), (frequency of word in year, num of years word occurs in)
		for i in range(len(terms)):
			yield (terms[i], word), (freqs[i], num)  
		
	#calculate formula		
	def TF(self, term, value):
		N = jobconf_from_env('myjob.settings.years')
		freq = value[0]
		logs = math.log10(int(N) / value[1])
		formula = freq * logs
		yield term[1], (term[0], formula)


	#output in form “term\t Year1,Weight1;Year2,Weight2;… …;Yeark,Weightk”		
	def output(self, word, yearTF):
		yearTF = list(yearTF)
		words = word + "\t"
		result = ""
		for years in yearTF:
			result = result + years[0] + "," + str(years[1]) + ";"
		result = result[:-1]
		
		#make sure a word exists
		if word:
			yield word, result
		
		
	def steps(self):
		return [
			MRStep(mapper=self.mapper_pairs,
				reducer=self.reducer_1),
			MRStep(
				reducer=self.terms),
			MRStep(
				mapper=self.TF,
				reducer=self.output)
		]
		
		
if __name__ == '__main__':
	TFID.run()
