#!/Users/anholm/Packages/anaconda3/bin/python  
# 3.9.13
# 
# Script by Melissa Anholm
# 2023.4.25



import os
import pandas
import datetime
import matplotlib
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import pyplot
import math


import GetGasData as ggd





oilrefinery_states = ['Alabama', 'Alaska', 'Arkansas', 'California', 'Delaware', 'Illinois', 'Indiana', 'Kentucky', 'Louisiana', 'Michigan', 'Minnesota', 'Missouri', 'Montana', 'New Jersey', 'New Mexico', 'North Dakota', 'Ohio', 'Oklahoma', 'Tennessee', 'Texas', 'Utah', 'Washington']



def deg_to_rad(theta_deg):
	theta_rad = theta_deg*math.pi / 180.0
	return theta_rad

def get_distance(lat1, lon1, lat2, lon2):
#	R = 6371.0 # km
	R = 3959.0 # mi
	lat1 = deg_to_rad(lat1)
	lat2 = deg_to_rad(lat2)
	dtheta = lat2 - lat1
	dphi = deg_to_rad(lon2 - lon1)
	Dr2 = (R*R) * ( (dtheta*dtheta) + (math.sin(lat2)-math.sin(lat1))*(math.sin(lat2)-math.sin(lat1)) * (dphi*dphi)  )
	return math.sqrt(Dr2)

def load_populations():
	pop_df = pandas.read_csv("CenterPop.csv", dtype={"COUNTYFP": str })
	pop_df.rename( columns={"COUNAME":  "County", "COUNTYFP":  "CountyCode", "POPULATION":  "Population", "LATITUDE":  "Pop_Latitude", "LONGITUDE":  "Pop_Longitude"}, inplace=True ) 
	pop_df["County"] = [ name+str(" County") for name in pop_df["County"] ]
	
	
	countypop_df = pandas.DataFrame( pop_df[["County", "CountyCode", "Population", "Pop_Latitude", "Pop_Longitude"]] )
	return countypop_df
	
def load_fires():
	fire_df = pandas.read_csv("FireData.csv")
	#	fire_df.rename(columns={"incident_acres_burned":  "Acres Burned"}) 
	
	
	fire_df["incident_date_created"]     = pandas.to_datetime(fire_df["incident_date_created"])
	fire_df["incident_dateonly_created"] = pandas.to_datetime(fire_df["incident_dateonly_created"])
	
	fire_df["incident_date_extinguished"]     = pandas.to_datetime(fire_df["incident_date_extinguished"])
	fire_df["incident_dateonly_extinguished"] = pandas.to_datetime(fire_df["incident_dateonly_extinguished"])
	
	fire_df["incident_date_last_update"] = pandas.to_datetime(fire_df["incident_date_last_update"])
	
	print("null count:  ", fire_df["incident_dateonly_created"].isna().sum() )
	
	fire_df_ = fire_df[fire_df["incident_dateonly_created"] > ggd.startdate].copy()
	
	print("null count:  ", fire_df_["incident_dateonly_created"].isna().sum() )
	
	#	fire_df_.sort_values(by=["incident_date_created"], inplace=True)
	#	fire_df_.sort_values(by=["incident_acres_burned"], inplace=True, ascending=False)
	
	# "incident_date_created"
	# "incident_dateonly_created"
	#incident_date_last_update, incident_date_extinguished, incident_dateonly_extinguished
	# "incident_longitude", "incident_latitude"
	# "incident_type"
	# "incident_county"
	# "incident_acres_burned"
	return fire_df_



def make_firedistances(fire_df, pop_df):
	firedistances_df = pandas.DataFrame()
	
	date_lit = fire_df["incident_dateonly_created"]
	date_ext = fire_df["incident_dateonly_extinguished"]
	fire_lat = fire_df["incident_latitude"]
	fire_lon = fire_df["incident_longitude"]
	A_burnt  = fire_df["incident_acres_burned"]
	
	county_lat = pop_df["Pop_Latitude"]
	county_lon = pop_df["Pop_Longitude"]
	
	#counties_list = ["DistTo "+county for county in pop_df["County"] ]
	dist_list = {}
	counties_fires = {}
	
	for county_ind in pop_df.index:
		anewcounty_list = []
		anewweight_list = []
		for fire_ind in fire_df.index:
			the_distance = get_distance( float(fire_df["incident_latitude"][fire_ind] ), float(fire_df["incident_longitude"][fire_ind] ), float( pop_df["Pop_Latitude"][county_ind] ), float( pop_df["Pop_Longitude"][county_ind] ) )
			anewcounty_list.append(the_distance)
			the_weight = ( float(fire_df["incident_acres_burned"][fire_ind] ) ) / the_distance
			anewweight_list.append(the_weight/1000.0)
		fire_df = pandas.concat([fire_df, pandas.Series(anewcounty_list).rename("DistTo "+str(pop_df["County"][county_ind]) ), pandas.Series(anewweight_list).rename("WeightFor "+str(pop_df["County"][county_ind]) ) ], axis=1)
		counties_fires[ str(pop_df["County"][county_ind]) ] = pandas.DataFrame( zip(anewcounty_list, A_burnt, anewweight_list, date_lit, date_ext), columns=["DistTo", "A_burnt", "WeightFor", "date_lit", "date_ext"] )
	#
	
	#	fire_df[ "DistTo "+str(pop_df["County"][county_ind]) ] = anewcounty_list
	#	fire_df[ "WeightFor "+str(pop_df["County"][county_ind]) ] = anewweight_list
	
	
	for county in counties_fires:
		#print(counties_fires[county])
		counties_fires[county].sort_values(by=["WeightFor"], inplace=True, ascending=False)
	
	#print(counties_fires["San Bernardino County"])
	
	return counties_fires
	
	



def main():
#	ggd.update_pickle_them()
	national_df, states_df, counties_df, provinces_df, cities_df = ggd.load_pickle_them()
#	print(national_df)
	mega_df = pandas.concat([national_df, states_df, counties_df, provinces_df], axis=1) 
#	print(mega_df)
	
	stateslist   = list(states_df.columns)
	countieslist = list(counties_df.columns)
	stupidcounties = ["Mono County"]
	
	
	counties_df = load_populations()
	fire_df = load_fires()
	counties_fires = make_firedistances(fire_df, counties_df)
	
	for county in counties_fires:
		print(county)
		print(counties_fires[county][["WeightFor", "date_lit", "date_ext"]])
		
	
	
#	fires_ax = counties_fires["San Bernardino County"].plot.scatter(x="date_lit", y="WeightFor" )
#	gas_ax   = mega_df.plot(ax = fires_ax, y=["California", "Los Angeles County", "San Bernardino County"])
	
	gas_ax   = mega_df.plot(y=["California", "Sonoma County", "San Bernardino County"])
	fires_ax = counties_fires["San Bernardino County"].plot.scatter(ax=gas_ax, x="date_lit", y="WeightFor", color="tab:orange", marker=".")
	fires_ax = counties_fires["Sonoma County"].plot.scatter(ax=gas_ax, x="date_lit", y="WeightFor", color="tab:green", alpha=0.5)
	
	lines_ax = pyplot.vlines(x=counties_fires["San Bernardino County"]["date_lit"], ymin=-1., ymax=100., color="tab:orange", alpha=0.5)
	
	
# #	return
# #	print("null count:  ", fire_df["incident_dateonly_created"].isna().sum() )
#
#
# #	print(fire_df)
# #	print("..")
#
# 	print(fire_df.sort_values(by=["DistTo San Bernardino County"], inplace=False)[["DistTo San Bernardino County", "incident_acres_burned"]][:10] )
# 	print("..")
# 	print(fire_df.sort_values(by=["WeightFor San Bernardino County"], inplace=False, ascending=False)[["WeightFor San Bernardino County", "incident_dateonly_created"]][:10] )
#
# 	# weightscolumns_list = ["WeightFor "+county for county in counties_df["County"] ]
# 	#
# 	# for weightstitle in weightscolumns_list:
# 	# 	print( fire_df.sort_values(by=[weightstitle], inplace=False, ascending=False)[[weightstitle, "incident_dateonly_created"]][:10] )
#
# 	return
	
	
	
	
	# print(states_list)
	
	# counties_df = load_populations()
	# print(counties_df)
	# return
	
#	national_df.plot(x=float(datetime('datetime')), y='price')
#	df.plot(y='price')

#	national_df.plot(y=['Canada', "USA"])
#	national_df.plot(y='Canada')
#	mega_df.plot(y=['Canada', "USA", "California"])
#	mega_df.plot(y=['Canada', "USA", "California", "New York"])
#	mega_df.plot(y=["USA", "California", "New York"])
	

#	mega_df.plot(y=[state for state in stateslist if state != "Hawaii"])

#	mega_df.plot(y=[state for state in stateslist if state != "Hawaii"])
#	print( sorted(oilrefinery_states))

#	mega_df.plot(y=["USA", "California", "Oregon", "Nevada", "Arizona"])   # adjacent states.
#	mega_df.plot(y=["USA", "California", "Texas", "New Mexico", "Washington", "Utah"])   # nearby refinery states.
	# Utah lags CA, 
	# Texas ---> USA, TN, OK, LA, AR
	# Washington lags CA.
	# Delaware is noisy.
	# Alaska lags CA.
	# IL --> IN, OH, KY has extra features that get damped out.
#	mega_df.plot(y=["USA", "California", "New Mexico", "Texas"])
#	mega_df.plot(y=["USA", "California", 'Illinois', "Texas"])  
	# Michigan is noisy.  May have relevant bumps.
	# Montana is v. smooth, and delayed rel. to TX and CA.
	# bumps that go Texas-->NM get magnified time-later in CA
	# MN, ND --> idek.
#	mega_df.plot(y=["USA", "California", "Texas", "New Mexico", "Canada"])  
	
	# Oregon:  delayed from CA.
	# Nevada:  about the same time as CA.
#	mega_df.plot(y=["USA", "California", "Texas", "New Mexico", "Oregon", "Nevada", "Arizona", "Washington", "British Columbia"])  
#	mega_df.plot(y=[county for county in countieslist if (county != "Colusa County" and county != "Calaveras County" and county != "Butte County" and county != "Amador County" and county != "Alpine County" and county != "Alameda County" and county != "Kings County" and county != "Kern County" and county != "Inyo County" and county != "Imperial County" and county != "Humboldt County" and county != "Glenn County" and county != "Fresno County" and county != "El Dorado County" and county != "Del Norte County" and county != "Contra Costa County" and county != "Mono County" and county != "Sierra County" and county != "Modoc County") ])
	
#	mega_df.plot(y=[state for state in oilrefinery_states])


	#mega_df.plot(y=[county for county in countieslist if (county not in stupidcounties) ])
	
	
	# # for every fire, how far is it from every county?
	# fire_df["rescaled_acres"] = fire_df["incident_acres_burned"]/100000. +2.
	# tmp_fire_df = fire_df[fire_df["rescaled_acres"] >= 2.001]
	# tmp_fire_df = tmp_fire_df[ tmp_fire_df["incident_county"] == "San Bernardino"]
	#
	# gas_ax = mega_df.plot(y=["California", "Los Angeles County", "San Bernardino County"])
	#
	# tmp_fire_df.plot.scatter(ax=gas_ax, x="incident_dateonly_created", y="rescaled_acres" )
	#
	# #print(fire_df[0:100][["incident_dateonly_created", "incident_county", "incident_acres_burned"]] )

	
	
#	print(fire_df)
	#print(fire_df["incident_date_created"])
	#print(fire_df["incident_dateonly_created"])
	#fire_df.plot( x=["incident_date_created"], y=["incident_dateonly_created"] )
	
	#print(fire_df[0:100][["incident_date_created", "incident_county", "incident_acres_burned"]] )
	
#	fire_df.plot.scatter( x=["incident_dateonly_created"], y=["incident_acres_burned"] )
#	fire_df.plot.scatter( x="incident_dateonly_created", y="incident_acres_burned" )
	
#	fire_df.set_index("incident_dateonly_created").resample("30D").count()["incident_acres_burned"].plot()
#	fire_df.set_index("incident_dateonly_created").resample("30D").sum()["incident_acres_burned"].plot()
	pyplot.show()
	
	return



# caldot - for traffic data.































if __name__ == '__main__':
	main()

#%%
