#!/Users/anholm/Packages/anaconda3/bin/python  
# 3.9.13
# 
# Script by Melissa Anholm
# 2023.4.06


import requests
import json
import pandas
import os
import datetime
import dill

startdate = pandas.Timestamp("2018-04-24")
today     = pandas.Timestamp.today()
yesterday = today + pandas.DateOffset(days=-1)


f_national = "gas_national_id.json"
f_state    = "gas_state_id.json"
f_province = "gas_province_id.json"
f_county   = "gas_county_id.json"
f_city     = "gas_CityCalifornia_id.json"


def assemble_counties_from_json():
	pwd = os.getcwd()
	full_fname = pwd+"/"+f_county
	
	with open(full_fname) as f:
		data = json.load(f)
	
	id_col   = [record["RegionID"]   for record in data["PriceRecords"]]
	name_col = [record["RegionName"] for record in data["PriceRecords"]]
	name_col = [ (name.replace("County - CA - ", ""))+str(" County") for name in name_col]
	
	df = pandas.DataFrame( zip(id_col, name_col), columns=["ID", "Name"] )
	return df

def assemble_states_from_json():
	pwd = os.getcwd()
	full_fname = pwd+"/"+f_state
	
	with open(full_fname) as f:
		data = json.load(f)
	
	id_col   = [record["RegionID"]   for record in data["PriceRecords"]]
	name_col = [record["RegionName"] for record in data["PriceRecords"]]
	#	type_col = [record["RegionTypeID"] for record in data["PriceRecords"]]       # it's 4.
	#	type_col = [record["RegionCountryCode"] for record in data["PriceRecords"]]  # it's 500000.
	
	df = pandas.DataFrame( zip(id_col, name_col), columns=["ID", "Name"] )
	return df

def assemble_provinces_from_json():
	pwd = os.getcwd()
	full_fname = pwd+"/"+f_province

	with open(full_fname) as f:
		data = json.load(f)
	
	id_col   = [record["RegionID"]   for record in data["PriceRecords"]]
	name_col = [record["RegionName"] for record in data["PriceRecords"]]
	#	type_col = [record["RegionTypeID"] for record in data["PriceRecords"]]       # it's 4.
	#	type_col = [record["RegionCountryCode"] for record in data["PriceRecords"]]  # it's 500001.
	
	df = pandas.DataFrame( zip(id_col, name_col), columns=["ID", "Name"] )
	return df

def assemble_citiescalifornia_from_json():
	pwd = os.getcwd()
	full_fname = pwd+"/"+f_city

	with open(full_fname) as f:
		data = json.load(f)
	
	id_col   = [record["RegionID"]   for record in data["PriceRecords"]]
	name_col = [record["RegionName"] for record in data["PriceRecords"]]
	
	df = pandas.DataFrame( zip(id_col, name_col), columns=["ID", "Name"] )
	return df

def assemble_national_from_json():
	pwd = os.getcwd()
	full_fname = pwd+"/"+f_national

	with open(full_fname) as f:
		data = json.load(f)
	
	id_col   = [record["RegionID"]       for record in data["PriceRecords"]]
	name_col = [record["RegionName"] for record in data["PriceRecords"]]  # MJA
	
	df = pandas.DataFrame( zip(id_col, name_col), columns=["ID", "Name"] )
	return df

def get_hist_prices_df(json_df, the_timewindow=13):  
	'''
	# this function is slow because we demand that the dataframes we output cover the *whole* date range, even if we're only updating one day's worth of information.  Whatever, that's a problem for later.  Maybe.
	
	#	Header isn't really needed....
	#	the_headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:109.0) Gecko/20100101 Firefox/111.0"}
	#	#	the_fueltype = 3  # normal gas.
	#
	#	response = requests.post('https://fuelinsights.gasbuddy.com/api/HighChart/GetHighChartRecords/', params={"regionID":[300005],"fuelType":3,"timeWindow":[13],"frequency":1}, headers=the_headers)
	#
	#	https://fuelinsights.gasbuddy.com/api/RegionQuickSearch/GetQuickSearch?countryID=500000&regionTypeID=4

	
		# nothing bigger than 13 seems to do anything.
		# the_timewindow = 13 # 5 years.  it's the default longest amount.
		# the_timewindow = 12 # 2 years.
		# the_timewindow = 11 # 9 months.
		# the_timewindow = 10 # 4 months?
		# the_timewindow = 9  # 1 year.
		# the_timewindow = 8  # 3 years.
		# the_timewindow = 7  # 2 years.
		# the_timewindow = 6  # 1 year.  again.
		# the_timewindow = 5  # 6 months.
		# the_timewindow = 4  # 1 month.
		# the_timewindow = 3  # 1 week
		# the_timewindow = 2  # yesterday.
		# the_timewindow = 1  # nothing.  (endpoint:  today.  but doesn't include endpoint.)
	'''
	
	the_url = 'https://fuelinsights.gasbuddy.com/api/HighChart/GetHighChartRecords/'
	the_fueltype = 3
	frequency = 1
	
	#print("from get_hist_prices_df(...), the_timewindow=", the_timewindow )
	
	queries_results = [ requests.post(the_url, json={"regionID":[the_id], "fuelType":3,"timeWindow":[the_timewindow],"frequency":1} ).json() for the_id in json_df["ID"] ]
	
	multi_json_thing = [ json.dumps(queries_results[an_index][0]["USList"]) for an_index in range(len(queries_results)) ]
	set_o_dfs = [ pandas.read_json(json_thing, orient="records") for json_thing in multi_json_thing ] 
	
	for df in set_o_dfs:
		df["datetime"] = pandas.to_datetime(df["datetime"])
		
	for location, df in zip(json_df["Name"], set_o_dfs):
		df.rename(columns={"price":  location}, inplace=True)
	
	new_date_df = pandas.DataFrame(index=pandas.date_range(start=startdate, end=today, freq="D"))
	
	multi_df = new_date_df.join( set_o_dfs[0].set_index("datetime") )
	for an_index in range(1, len(queries_results)):
		multi_df =multi_df.join( set_o_dfs[an_index].set_index("datetime") ) 
	
	return multi_df


def load_national_from_pickle():
	pwd = os.getcwd()+"/"
	
	fname = "pickle_national.pkd"
	full_fname = pwd+fname
	fexists = os.path.exists(full_fname)
	
	if fexists:
		with open(full_fname, 'rb') as f:
			national_df = dill.load(f)
	else:
		print("Could not find file: ", fname)
		national_df = pandas.DataFrame()
	
	return national_df

def load_states_from_pickle():
	pwd = os.getcwd()+"/"
	
	fname = "pickle_states.pkd"
	full_fname = pwd+fname
	fexists = os.path.exists(full_fname)
	
	if fexists:
		with open(full_fname, 'rb') as f:
			states_df = dill.load(f)
	else:
		print("Could not find file: ", fname)
		states_df = pandas.DataFrame()
	
	return states_df

def load_counties_from_pickle():
	pwd = os.getcwd()+"/"
	
	fname = "pickle_counties.pkd"
	full_fname = pwd+fname
	fexists = os.path.exists(full_fname)

	if fexists:
		with open(full_fname, 'rb') as f:
			counties_df = dill.load(f)
	else:
		print("Could not find file: ", fname)
		counties_df = pandas.DataFrame()
	
	return counties_df

def load_provinces_from_pickle():
	pwd = os.getcwd()+"/"
	
	fname = "pickle_provinces.pkd"
	full_fname = pwd+fname
	fexists = os.path.exists(full_fname)

	if fexists:
		with open(full_fname, 'rb') as f:
			provinces_df = dill.load(f)
	else:
		print("Could not find file: ", fname)
		provinces_df = pandas.DataFrame()
	
	return provinces_df

def load_cities_from_pickle():
	pwd = os.getcwd()+"/"
	
	fname = "pickle_cities.pkd"
	full_fname = pwd+fname
	fexists = os.path.exists(full_fname)

	if fexists:
		with open(full_fname, 'rb') as f:
			cities_df = dill.load(f)
	else:
		print("Could not find file: ", fname)
		cities_df = pandas.DataFrame()
	
	return cities_df

def load_pickle_them():
	# if the files aren't saved, the dataframe is returned empty.
	print("Loading data from pickled files...")
	national_df  = load_national_from_pickle()
	states_df    = load_states_from_pickle()
	counties_df  = load_counties_from_pickle()
	#
	provinces_df = load_provinces_from_pickle()
	cities_df    = load_cities_from_pickle()
	print("\t...done.")
	#
	return national_df, states_df, counties_df, provinces_df, cities_df


def dl_national(timewindow=13):
	nationalinfo_df = assemble_national_from_json()
	national_df = get_hist_prices_df(nationalinfo_df, the_timewindow=timewindow)
	return national_df

def dl_states(timewindow=13):
	statesinfo_df = assemble_states_from_json()
	states_df = get_hist_prices_df(statesinfo_df, the_timewindow=timewindow)
	return states_df

def dl_counties(timewindow=13):
	countiesinfo_df = assemble_counties_from_json()
	counties_df = get_hist_prices_df(countiesinfo_df, the_timewindow=timewindow)
	return counties_df
	
def dl_provinces(timewindow=13):
	provincesinfo_df = assemble_provinces_from_json()
	provinces_df = get_hist_prices_df(provincesinfo_df, the_timewindow=timewindow)
	return provinces_df

def dl_cities(timewindow=13):
	citiesinfo_df = assemble_citiescalifornia_from_json()
	cities_df = get_hist_prices_df(citiesinfo_df, the_timewindow=timewindow)
	return cities_df

def dl_by_stringparam(stringparam, window=13):
	if stringparam == "national":
		df = dl_national(timewindow=window)
	elif stringparam == "states" or stringparam == "state":
		df = dl_states(timewindow=window)
	elif stringparam == "counties" or stringparam == "county":
		df = dl_counties(timewindow=window)
	elif stringparam == "provinces" or stringparam == "province":
		df = dl_provinces(timewindow=window)
	elif stringparam == "cities" or stringparam == "city":
		df = dl_cities(timewindow=window)
	else:
		print("Parameter", stringparam, "not recognized.  :( " )
		df = pandas.DataFrame()
	return df
	

def download_and_pickle(overwrite=False):
	pwd = os.getcwd()+"/"
	
	if( overwrite==True):
		print("WARNING:  Overwrite allowed.")
	
	full_fname = pwd+"pickle_national.pkd"
	if( overwrite or not os.path.exists(full_fname) ):
		print("Downloading national data.")
		national_df  = dl_national()
		with open(full_fname, 'wb') as f:
			dill.dump(national_df, f)
	else:
		print("Skipping download to:  pickle_national.pkd")
		
	full_fname = pwd+"pickle_states.pkd"
	if( overwrite or not os.path.exists(full_fname) ):
		print("Downloading state data.")
		states_df    = dl_states()
		with open(full_fname, 'wb') as f:
			dill.dump(states_df, f)
	else:
		print("Skipping download to:  pickle_states.pkd")
		
	full_fname = pwd+"pickle_counties.pkd"
	if( overwrite or not os.path.exists(full_fname) ):
		print("Downloading county data.")
		counties_df  = dl_counties()
		with open(full_fname, 'wb') as f:
			dill.dump(counties_df, f)
	else:
		print("Skipping download to:  pickle_counties.pkd")
		
	full_fname = pwd+"pickle_cities.pkd"
	if( overwrite or not os.path.exists(full_fname) ):
		print("Downloading city data.")
		cities_df    = dl_cities()
		with open(full_fname, 'wb') as f:
			dill.dump(cities_df, f)
	else:
		print("Skipping download to:  pickle_cities.pkd")
		
	full_fname = pwd+"pickle_provinces.pkd"
	if( overwrite or not os.path.exists(full_fname) ):
		print("Downloading provincial data.")
		provinces_df = dl_provinces()
		with open(full_fname, 'wb') as f:
			dill.dump(provinces_df, f)
	else:
		print("Skipping download to:  pickle_provinces.pkd")
		
	return

def update_by_stringparam(the_db_str, orig_df):
	the_timecode = 13
	#
	N_size = len(orig_df)
	if(N_size == 0):
		new_df = dl_by_stringparam(the_db_str, window=the_timecode)
		return new_df
	else:
		date_updated = orig_df.iloc[N_size-1, :].name
		a_datedelta = today - date_updated
		#
		if a_datedelta < pandas.Timedelta(1, "d"):  
			print("The", the_db_str, "dataframe is already up to date.")
			return orig_df
		elif a_datedelta < pandas.Timedelta(28, "d"):     # smallest possible month
			print("It's been less than a month!")
			the_timecode = 4
		elif a_datedelta < pandas.Timedelta(365, "d"):    # smallest year.
			print("It's been less than a year!")
			the_timecode = 6
		elif a_datedelta < pandas.Timedelta(365*3, "d"):  # smallest 3 years.
			print("It's been less than 3 years!")
			the_timecode = 8
		elif a_datedelta < pandas.Timedelta(365*5, "d"):  # smaller than (slightly less than) 5 years.
			print("It's been less than 5 years!")
			the_timecode = 13
		else:
			print("It's been more than 5 years.")
			the_timecode = 13
			new_df = dl_by_stringparam(the_db_str, window=the_timecode)
			return new_df
		#
	#
	new_df = dl_by_stringparam(the_db_str, window=the_timecode)
	
	#chop off the most recent date from the original df:
	most_recent_saved = orig_df.iloc[N_size-2, :].name
	orig_df = orig_df.iloc[:N_size-1, :]  # chop off "date updated"

	i_lastadded = new_df.index.get_loc(date_updated) 
	new_df = new_df.iloc[i_lastadded:, :]
	
	joint_df = pandas.concat([orig_df, new_df])
	return joint_df
	#

def picklesave_by_stringparam(stringparam, the_df, overwrite=True):
	if stringparam == "national":
		pass
	elif stringparam == "states" or stringparam == "state":
		stringparam = "states"
	elif stringparam == "counties" or stringparam == "county":
		stringparam = "counties"
	elif stringparam == "provinces" or stringparam == "province":
		stringparam = "provinces"
	elif stringparam == "cities" or stringparam == "city":
		stringparam = "cities"
	else:
		print("Parameter", stringparam, "not recognized.  :( " )
		return
	
	pwd = os.getcwd()+"/"
	fname = "pickle_"+stringparam+".pkd"
	
	full_fname = pwd+fname
	if not os.path.exists(full_fname):
		print("File", fname, "does not exist and will be created.")
	elif( overwrite==True ):
		print("File", fname, "already exists but will be overwritten.")
	else:
		print("File", fname, "already exists and cannot be updated.")
		return
	
	with open(full_fname, 'wb') as f:
		dill.dump(the_df, f)
		
	return



def update_pickle_them():
	# load from pickled files:
	national_df, states_df, counties_df, provinces_df, cities_df = load_pickle_them()
	
	# update the dataframes:
	national_df  = update_by_stringparam("national",  national_df)
	states_df    = update_by_stringparam("states",    states_df)
	counties_df  = update_by_stringparam("counties",  counties_df)
	provinces_df = update_by_stringparam("provinces", provinces_df)
	cities_df    = update_by_stringparam("cities",    cities_df)
	
	# re-save dataframes to files:
	picklesave_by_stringparam("national",  national_df)
	picklesave_by_stringparam("states",    states_df)
	picklesave_by_stringparam("counties",  counties_df)
	picklesave_by_stringparam("provinces", provinces_df)
	picklesave_by_stringparam("cities",    cities_df)
	
	return
	#
	
	
	
	

























	
	

def main():
	print ("Hello, world!")
#	get_pickle_them()

	update_pickle_them()
	national_df, states_df, counties_df, provinces_df, cities_df = load_pickle_them()
	print(national_df)
	
	return
	
	
	
	
# 	# response = requests.post('https://fuelinsights.gasbuddy.com/api/HighChart/GetHighChartRecords/', json={"regionID":[300005],"fuelType":3,"timeWindow":[the_timewindow],"frequency":1})
#
#
# #	print(response.headers['content-type'])
# #	print(type(response))
# #	print(type(response.json()))
#
# 	newthing = response.json()
# #	print(newthing)
# #	print(newthing[0]["USList"])
#
# 	print(type(newthing[0]["USList"]) )
# 	df = pandas.DataFrame(newthing[0]["USList"])
#
# 	print( type( df['datetime']) )
# 	print( df['datetime'].dtype )
#
# 	return

	df.plot(x=float(datetime('datetime')), y='price')
#	df.plot(y='price')
	
	pyplot.show()
	print(df)
	
#	countryID=500000
#	regionTypeID=12
	
	print(response.headers['content-type'])
#	print(response.text)
	print(response.text[:300])
#	print(response.status_code)
	print(response.reason)
	print(response.url)
#	print(response.content[:300])
	
	
	
	return



# caldot - for traffic data.































if __name__ == '__main__':
	main()

#%%
