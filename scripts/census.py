import pandas as pd
import os
import numpy

# R code to pull census data from all countries using US Census API
'''Sys.setenv(CENSUS_KEY="08c8ba8ae8bef6d1ca039ff07d44028a28190797")
install.packages("censusapi")
library(censusapi)
pop <- getCensus(name = "timeseries/idb/1year",
                          vars = c("NAME", "FIPS","POP","SEX"),
                          time = 2020,
                          AGE = "1:100")
'''
# Census of data

path = os.path.dirname(__file__)
print(path.rfind('/'))
CensusPath=path[0:path.rfind('/')]+'/rawData/Census/'

colsUS=['NAME','SEX','AGE','POPEST2018_CIV']
print(path)

# Getting the data for US states

dfUSCensusPop=pd.read_csv(CensusPath+'sc-est2018-agesex-civ.csv')

# discard the aggregated data for males and females
mask = (dfUSCensusPop['SEX'] != 0)
dfUSMF1 = dfUSCensusPop.loc[mask]

# discard the aggregated data for all ages
mask2 = (dfUSMF1['AGE'] != 999)
dfUSMF2 = dfUSMF1.loc[mask2]

# discard the aggregated data for US as a whole
mask3 = (dfUSMF2['STATE'] != 0)
dfUSMF = dfUSMF2.loc[mask3]
print(dfUSMF.count())
dfUSMF=dfUSMF[colsUS]
g = {'POPEST2018_CIV':['sum'],'SEX':['min'],'AGE':['max']}


CensusUSCols=dfUSMF.columns
new_ColumnsUS=[]
df_US_all_new=pd.DataFrame()
for c in CensusUSCols:
    if c != ('NAME',''):
        df_US_all_new[c[0]] = dfUSMF[c]
    else:
        df_US_all_new['NAME']=dfUSMF[c]
new_all_Columns=df_US_all_new.columns
print(new_all_Columns)


print(df_US_all_new.count())

# dataframe for Males
mask1 = (dfUSMF['SEX'] == 1)
dfMale = dfUSMF.loc[mask1]
print(dfMale.count())

# dataframe for  Females
mask2 = (dfUSMF['SEX'] == 2)
dfFemale = dfUSMF.loc[mask2]
print(dfFemale.count())

# dataframe for  age 0 - 14
mask3 = (dfUSMF['AGE'] < 15)
df14 = dfUSMF.loc[mask3]

# dataframe for  age 15 - 64
mask4 = (dfUSMF['AGE'] > 14)
df15 = dfUSMF.loc[mask4]

mask5 = (dfUSMF['AGE'] < 65)
df15_65 = df15.loc[mask5]
print(df15_65)

# dataframe for age >65
mask6 = (dfUSMF['AGE'] > 64)
df65 = dfUSMF.loc[mask6]


g = {'POPEST2018_CIV':['sum'],'SEX':['max']}

# Aggregate on US state level
df=dfUSMF.groupby(["NAME"]).agg(g).reset_index()
dfM=dfMale.groupby(["NAME"]).agg(g).reset_index()
dfF=dfFemale.groupby(["NAME"]).agg(g).reset_index()
df_FM=dfUSMF.groupby(["NAME"]).agg(g).reset_index()
df_14=df14.groupby(["NAME"]).agg(g).reset_index()
df_15=df15_65.groupby(["NAME"]).agg(g).reset_index()
df_65=df65.groupby(["NAME"]).agg(g).reset_index()


# create csv file for US State level with 'time','Country','State','Population','MFRatio','0-14','15-65','65+'
df_US_all_new['Population']=df_US_all_new['POPEST2018_CIV']
df_US_all_new['Male'] = dfM[('POPEST2018_CIV','sum')]
df_US_all_new['Female'] = dfF[('POPEST2018_CIV','sum')]
df_US_all_new['0-14'] = df_14[('POPEST2018_CIV','sum')]
df_US_all_new['15-65'] = df_15[('POPEST2018_CIV','sum')]
df_US_all_new['65+'] = df_65[('POPEST2018_CIV','sum')]
df_US_all_new['Country']='US'
df_US_all_new['time']='2018'
df_US_all_new['MFRatio']=df_US_all_new['Male']/df_US_all_new['Female']
df_US_all_new['State']=df_US_all_new['NAME']
#df_US_all_new.to_csv ('df_US_all.csv', index = False, header=True)
cols=['time','Country','State','Population','MFRatio','0-14','15-65','65+']
df_US=pd.DataFrame()
df_US[cols]=df_US_all_new[cols]
df_US.to_csv ('df_US_all.csv', index = False, header=True)


# Census of all countries (same as above but for all countries)
dfCensusPop=pd.read_csv(CensusPath+'pop.csv')
print(dfCensusPop.count())
mask1 = (dfCensusPop['SEX'] == 1)
dfMale = dfCensusPop.loc[mask1]
print(dfMale.count())

mask2 = (dfCensusPop['SEX'] == 2)
dfFemale = dfCensusPop.loc[mask2]
print(dfFemale.count())

mask22 = (dfCensusPop['SEX'] != 0)
dfMF = dfCensusPop.loc[mask22]
print(dfMF.count())

mask3 = (dfMF['AGE'] < 15)
df14 = dfMF.loc[mask3]

mask4 = (dfMF['AGE'] > 14)
df15 = dfMF.loc[mask4]

mask5 = (dfMF['AGE'] < 65)
df15_65 = df15.loc[mask5]
print(df15_65)

mask6 = (dfCensusPop['AGE'] > 64)
df65 = dfMF.loc[mask6]


g = {'POP':['sum'],'SEX':['max'],'time':['max']}

df=dfMF.groupby(["NAME"]).agg(g).reset_index()
dfM=dfMale.groupby(["NAME"]).agg(g).reset_index()
dfF=dfFemale.groupby(["NAME"]).agg(g).reset_index()
df_FM=dfMF.groupby(["NAME"]).agg(g).reset_index()
df_14=df14.groupby(["NAME"]).agg(g).reset_index()
df_15=df15_65.groupby(["NAME"]).agg(g).reset_index()
df_65=df65.groupby(["NAME"]).agg(g).reset_index()

CensusCols=df.columns
new_Columns=[]
df_all_new=pd.DataFrame()
for c in CensusCols:
    if c != ('NAME',''):
        # replace the -INFINITY numbers for the aggregated features with nan again after grouping per car per hour
        df_all_new[c[0]] = df[c]
    else:
        df_all_new['NAME']=df[c]
new_all_Columns=df_all_new.columns
print(new_all_Columns)

df_all_new['Population']=df_all_new['POP']
df_all_new['Male'] = dfM[('POP','sum')]
df_all_new['Female'] = dfF[('POP','sum')]
df_all_new['0-14'] = df_14[('POP','sum')]
df_all_new['15-65'] = df_15[('POP','sum')]
df_all_new['65+'] = df_65[('POP','sum')]
df_all_new['Country']=df_all_new['NAME']
df_all_new['MFRatio']=df_all_new['Male']/df_all_new['Female']
df_all_new['State']=''
df_all_new.to_csv ('df_all.csv', index = False, header=True)
cols=['time','Country','State','Population','MFRatio','0-14','15-65','65+']
df_Countries=pd.DataFrame()
df_Countries[cols]=df_all_new[cols]
df_Countries.to_csv ('df.csv', index = False, header=True)