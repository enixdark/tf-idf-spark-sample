import pandas
import re
import sys


args = sys.argv[1]
#import ipdb;ipdb.set_trace()

data = pandas.read_csv(args,header=None)
dataframes = data[[3,28]]
clean_data = map(lambda text: re.sub(re.compile('\\s+'),' ', re.sub( re.compile('(<[^<]+?>|&[\w#+\d]+;|[\\t\\s]+)'), ' ', str(text))) , data[28] )
#import ipdb;ipdb.set_trace()
dataframes[28] = clean_data
dataframes.columns = ['company_name','content']
dataframes.to_csv('input.csv',index=None)

