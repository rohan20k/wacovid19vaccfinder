#!/usr/bin/env python
# coding: utf-8

# ## IMT 563 
# ### Group 7 | Covid -19 Vaccination Info 
# ### Authors - Julius Coleman, Rohan Khurana and Vaibhav Rao 

# In[1]:


# Importing useful packages and libraries 
import pandas as pd
import numpy as np 
import datetime
from datetime import datetime 
import snowflake.connector 
from snowflake import sqlalchemy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine,inspect
import pytz 
from calendar import monthrange 
import re 
from tqdm import tqdm 


# In[2]:


# Establishing Snowflake Connection Parameters 

engine = create_engine(URL(
                            account = 'tca69088',
                            role = 'SYSADMIN',
                            user = 'Group7',
                            password = 'Jrv12345!',
                            database = 'IMT_DB',
                            schema = 'PUBLIC',
                            ))


# ### Importing Files

# #### Comment - HERE I HAVE NOT REMOVED UNASSIGNED VALUES

# In[3]:


### Writing a function to do the same 
### - So the function should take in file_path,sheet_name,list_non_group

def load_wa_chd(file_path,sheet_name,list_columns,list_group,agg_value):
    df = pd.ExcelFile(file_path)
    df_s = pd.read_excel(df, sheet_name = sheet_name)
    df_s = df_s[list_columns]
    df_s = df_s.groupby(list_group,as_index=False)[agg_value].sum()
    df_s = df_s[df_s['County'] != 'Unassigned']
    df_s = df_s.reset_index(drop=True)
    return df_s 


# #### 7th MARCH CASES

# In[4]:


file_path_wa_c_7 = '/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_WA_COVID19_Cases_Hospitalizations_Deaths (2).xlsx'
sheet_name_wa_c_7 = 'Cases'
list_columns_wa_c_7 = ['County','TotalCases']
list_group_wa_c_7 = ['County']
agg_value_c = 'TotalCases'
wa_chd_7_cases = load_wa_chd(file_path_wa_c_7,sheet_name_wa_c_7,list_columns_wa_c_7,list_group_wa_c_7
                              ,agg_value_c)


# #### 7th MARCH HOSPITALIZATIONS 

# In[5]:


file_path_wa_h_7 = '/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_WA_COVID19_Cases_Hospitalizations_Deaths (2).xlsx'
sheet_name_wa_h_7 = 'Hospitalizations'
list_columns_wa_h_7 = ['County','Hospitalizations']
list_group_wa_h_7 = ['County']
agg_value_h = 'Hospitalizations'
wa_chd_7_hospitalizations = load_wa_chd(file_path_wa_h_7,sheet_name_wa_h_7,list_columns_wa_h_7,list_group_wa_h_7
                              ,agg_value_h)


# #### 7th MARCH DEATHS 

# In[6]:


file_path_wa_d_7 = '/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_WA_COVID19_Cases_Hospitalizations_Deaths (2).xlsx'
sheet_name_wa_d_7 = 'Deaths'
list_columns_wa_d_7 = ['County','Deaths']
list_group_wa_d_7 = ['County']
agg_value_d = 'Deaths'
wa_chd_7_deaths = load_wa_chd(file_path_wa_d_7,sheet_name_wa_d_7,list_columns_wa_d_7,list_group_wa_d_7
                              ,agg_value_d)


# #### 21st FEB CASES

# In[7]:


file_path_wa_c_21 = '/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_WA_COVID19_Cases_Hospitalizations_Deaths.xlsx'
sheet_name_wa_c_21 = 'Cases'
list_columns_wa_c_21 = ['County','TotalCases']
list_group_wa_c_21 = ['County']
agg_value_c = 'TotalCases'
wa_chd_21_cases = load_wa_chd(file_path_wa_c_21,sheet_name_wa_c_21,list_columns_wa_c_21,list_group_wa_c_21
                              ,agg_value_c)


# #### 21st FEB HOSPITALIZATIONS

# In[8]:


file_path_wa_h_21 = '/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_WA_COVID19_Cases_Hospitalizations_Deaths.xlsx'
sheet_name_wa_h_21 = 'Hospitalizations'
list_columns_wa_h_21 = ['County','Hospitalizations']
list_group_wa_h_21 = ['County']
agg_value_h = 'Hospitalizations'
wa_chd_21_hospitalizations = load_wa_chd(file_path_wa_h_21,sheet_name_wa_h_21,list_columns_wa_h_21,list_group_wa_h_21
                              ,agg_value_h)


# #### 21st FEB DEATHS

# In[9]:


file_path_wa_d_21 = '/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_WA_COVID19_Cases_Hospitalizations_Deaths.xlsx'
sheet_name_wa_d_21 = 'Deaths'
list_columns_wa_d_21 = ['County','Deaths']
list_group_wa_d_21 = ['County']
agg_value_d = 'Deaths'
wa_chd_21_deaths = load_wa_chd(file_path_wa_d_21,sheet_name_wa_d_21,list_columns_wa_d_21,list_group_wa_d_21
                              ,agg_value_d)


# ### Zip Level Data

# In[10]:


def load_wa_zip_chd(file_path,sheet_name,list_columns):
    df = pd.ExcelFile(file_path)
    df_s = pd.read_excel(df, sheet_name = sheet_name)
    df_s = df_s[list_columns]
    df_s = df_s.drop(0)
    df_s.reset_index(drop=True,inplace=True)
    return df_s 


# ####  7th March 

# In[11]:


file_path_wa_zip_c_7 = "/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_overall-counts-rates-geography-mar-3.xlsx"
sheet_name_wa_zip_c_7 = 'ZIP'
list_columns_wa_zip_c_7 =['Location_Name','Positives']
wa_chd_zip_7_cases = load_wa_zip_chd(file_path_wa_zip_c_7,sheet_name_wa_zip_c_7,list_columns_wa_zip_c_7)


# In[12]:


file_path_wa_zip_d_7 = "/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_overall-counts-rates-geography-mar-3.xlsx"
sheet_name_wa_zip_d_7 = 'ZIP'
list_columns_wa_zip_d_7 =['Location_Name','Deaths']
wa_chd_zip_7_deaths = load_wa_zip_chd(file_path_wa_zip_d_7,sheet_name_wa_zip_d_7,list_columns_wa_zip_d_7)


# In[13]:


file_path_wa_zip_h_7 = "/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_overall-counts-rates-geography-mar-3.xlsx"
sheet_name_wa_zip_h_7 = 'ZIP'
list_columns_wa_zip_h_7 =['Location_Name','Hospitalizations']
wa_chd_zip_7_hospitalizations = load_wa_zip_chd(file_path_wa_zip_h_7,sheet_name_wa_zip_h_7,list_columns_wa_zip_h_7)


# #### 21st Feb 

# In[14]:


file_path_wa_zip_c_21 = "/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_overall-counts-rates-geography-feb-17 (1).xlsx"
sheet_name_wa_zip_c_21 = 'ZIP'
list_columns_wa_zip_c_21 =['Location_Name','Positives']
wa_chd_zip_21_cases = load_wa_zip_chd(file_path_wa_zip_c_21,sheet_name_wa_zip_c_21,list_columns_wa_zip_c_21)


# In[15]:


file_path_wa_zip_d_21 = "/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_overall-counts-rates-geography-feb-17 (1).xlsx"
sheet_name_wa_zip_d_21 = 'ZIP'
list_columns_wa_zip_d_21 =['Location_Name','Deaths']
wa_chd_zip_21_deaths = load_wa_zip_chd(file_path_wa_zip_d_21,sheet_name_wa_zip_d_21,list_columns_wa_zip_d_21)


# In[16]:


file_path_wa_zip_h_21 = "/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_overall-counts-rates-geography-feb-17 (1).xlsx"
sheet_name_wa_zip_h_21 = 'ZIP'
list_columns_wa_zip_h_21 =['Location_Name','Hospitalizations']
wa_chd_zip_21_hospitalizations = load_wa_zip_chd(file_path_wa_zip_h_21,sheet_name_wa_zip_h_21,list_columns_wa_zip_h_21)


# ### Covid County Vaccinations 

# ### 7th March 

# In[17]:


wa_vacc_7 = pd.read_excel(r'/Users/rohan20k/Desktop/data_imt_563/7th Mar 2021/7_Mar_Vaccination_County_Level_Counts.xlsx')
wa_vacc_7 = wa_vacc_7[['County','People Initiating Vaccination']]


# In[18]:


wa_vacc_7 = wa_vacc_7.drop([39,40]) 


# In[19]:


wa_vacc_7['County'] = wa_vacc_7['County'].apply(lambda x: x+' County')


# ### 21st February

# In[20]:


wa_vacc_21 = pd.read_excel(r'/Users/rohan20k/Desktop/data_imt_563/21st Feb 2021/21_Feb_Vaccination_County_Level_Counts.xlsx')
wa_vacc_21 = wa_vacc_21[['County','People Initiating Vaccination']]


# In[21]:


wa_vacc_21 = wa_vacc_21.drop([39,40])


# In[22]:


wa_vacc_21['County'] = wa_vacc_21['County'].apply(lambda x: x+' County')


# # Table Creation 

# ### State_Name Creation 

# In[23]:


state_data = [['1','Washington State']]


# In[24]:


df_state_name = pd.DataFrame(state_data, columns = ["State_ID", "State_Name"])


# In[25]:


state_name = df_state_name.copy()


# ### County_Name Creation 

# In[26]:


county_raw = wa_chd_21_cases.copy()

county_raw.drop(columns=['TotalCases'], inplace = True)

county_raw_primary_key_list = [item for item in range(100,139)]

df_county_name = county_raw.copy()

df_county_name['State_ID'] = 1

df_county_name["County_ID"] = county_raw_primary_key_list

df_county_name = df_county_name[['County_ID','State_ID','County']]

df_county_name.rename(columns = {'County':'County_Name'},inplace = True)

county_name = df_county_name.copy()


# ### Zip_Table

# In[46]:


zip_data_raw = pd.read_excel(r'/Users/rohan20k/Desktop/data_imt_563/Washington_ZipCodes.xlsx')

zip_data_raw['County'] = zip_data_raw['County'].apply(lambda x:str(x) +' County')

zip_data_raw_merged = zip_data_raw.merge(df_county_name, left_on='County', right_on='County_Name')

df_zip_table = zip_data_raw_merged[['County_ID','Zip Code']]

zip_raw_primary_key_list = [item for item in range(1000,1732)]

df_zip_table.loc[:,'ZIP_ID'] = zip_raw_primary_key_list

df_zip_table = df_zip_table[['ZIP_ID','County_ID','Zip Code']]

zip_table = df_zip_table.rename({'Zip Code':'ZIP_Code'},axis = 1)


# ## Data Consistency 

# ```
# The various tables we have - 
# 
# * wa_chd_7_cases 
# ['County','TotalCases']  
# 
# * wa_chd_7_hospitalizations 
# ['County','Hospitalizations']
# 
# * wa_chd_7_deaths 
# ['County','Deaths']
# 
# * wa_chd_21_cases 
# ['County','TotalCases']
# 
# * wa_chd_21_hospitalizations 
# ['County','Hospitalizations']
# 
# * wa_chd_21_deaths 
# ['County','Deaths']
# 
# * wa_chd_zip_7_cases 
# ['Location_Name','Positives']
# 
# * wa_chd_zip_7_hospitalizations 
# ['Location_Name','Hospitalizations']
# 
# * wa_chd_zip_7_deaths 
# ['Location_Name','Deaths']
# 
# * wa_chd_zip_21_cases 
# ['Location_Name','Positives']
# 
# * wa_chd_zip_21_hospitalizations 
# ['Location_Name','Hospitalizations']
# 
# * wa_chd_zip_21_deaths 
# ['Location_Name','Deaths']
# 
# ```

# In[28]:


wa_chd_7_cases.loc[16,'TotalCases'],  wa_chd_zip_7_cases['Positives'].sum()

wa_chd_7_cases.loc[16,'TotalCases'] = 81654

wa_chd_7_cases.loc[16,'TotalCases'],  wa_chd_zip_7_cases['Positives'].sum()

wa_chd_7_hospitalizations.loc[16,'Hospitalizations'], wa_chd_zip_7_hospitalizations['Hospitalizations'].sum()

wa_chd_7_hospitalizations.loc[16,'Hospitalizations'] = 5070

wa_chd_7_hospitalizations.loc[16,'Hospitalizations'], wa_chd_zip_7_hospitalizations['Hospitalizations'].sum()

wa_chd_7_deaths.loc[16,'Deaths'],wa_chd_zip_7_deaths['Deaths'].sum()

wa_chd_7_deaths.loc[16,'Deaths'] = 1394

wa_chd_7_deaths.loc[16,'Deaths'],wa_chd_zip_7_deaths['Deaths'].sum()

wa_chd_21_cases.loc[16,'TotalCases'],  wa_chd_zip_21_cases['Positives'].sum()

wa_chd_21_cases.loc[16,'TotalCases'] = 79457

wa_chd_21_cases.loc[16,'TotalCases'],  wa_chd_zip_21_cases['Positives'].sum()

wa_chd_21_hospitalizations.loc[16,'Hospitalizations'], wa_chd_zip_21_hospitalizations['Hospitalizations'].sum()

wa_chd_21_hospitalizations.loc[16,'Hospitalizations'] = 4970

wa_chd_21_hospitalizations.loc[16,'Hospitalizations'], wa_chd_zip_21_hospitalizations['Hospitalizations'].sum()

wa_chd_21_deaths.loc[16,'Deaths'],wa_chd_zip_21_deaths['Deaths'].sum()

wa_chd_21_deaths.loc[16,'Deaths'] = 1308

wa_chd_21_deaths.loc[16,'Deaths'],wa_chd_zip_21_deaths['Deaths'].sum()


# ### Table Creation Continued

# In[29]:


### Covid County Cases on 7th March


covid_countycases_7 = wa_chd_7_cases.copy()

covid_countycases_7_primary_key_list = [item for item in range(301,340)]

covid_countycases_7['CountyCases_ID'] = covid_countycases_7_primary_key_list

covid_countycases_7_merged = covid_countycases_7.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countycases_7_merged.rename({'TotalCases':'CountyCases_Sum'},axis = 1,inplace = True)

covid_countycases_7_merged = covid_countycases_7_merged[['CountyCases_ID','County_ID','CountyCases_Sum']]

covid_countycases_7_merged.loc[:,'LastUpdated'] = '2021-03-07'

### Covid County Cases on 21st February

covid_countycases_21 = wa_chd_21_cases.copy()

covid_countycases_21_primary_key_list = [item for item in range(262,301)]

covid_countycases_21['CountyCases_ID'] = covid_countycases_21_primary_key_list

covid_countycases_21_merged = covid_countycases_21.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countycases_21_merged.rename({'TotalCases':'CountyCases_Sum'},axis = 1,inplace = True)

covid_countycases_21_merged = covid_countycases_21_merged[['CountyCases_ID','County_ID','CountyCases_Sum']]

covid_countycases_21_merged.loc[:,'LastUpdated'] = '2021-02-21'

covid_countycases = covid_countycases_7_merged.append(covid_countycases_21_merged)

covid_countycases = covid_countycases.sort_values(by='CountyCases_ID').reset_index(drop=True)


# #### Adding the missing counties to the deaths table

# In[30]:


new_row = [{'County':'San Juan County','Deaths':0},{'County':'Wahkiakum County','Deaths':0}]


# In[31]:


wa_chd_21_deaths = wa_chd_21_deaths.append(new_row,ignore_index=True).sort_values(by='County').reset_index(drop=True)


# In[32]:


wa_chd_7_deaths = wa_chd_7_deaths.append(new_row,ignore_index=True).sort_values(by='County').reset_index(drop=True)


# In[33]:


### Covid County Deaths on 7th March

covid_countydeaths_7 = wa_chd_7_deaths.copy()

covid_countydeaths_7_primary_key_list = [item for item in range(3001,3040)]

covid_countydeaths_7['CountyDeaths_ID'] = covid_countydeaths_7_primary_key_list

covid_countydeaths_7_merged = covid_countydeaths_7.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countydeaths_7_merged.rename({'Deaths':'CountyDeaths_Sum'},axis = 1,inplace = True)

covid_countydeaths_7_merged.columns

covid_countydeaths_7_merged = covid_countydeaths_7_merged[['CountyDeaths_ID','County_ID','CountyDeaths_Sum']]

covid_countydeaths_7_merged.loc[:,'LastUpdated'] = '2021-03-07'

### Covid County Deaths on 21st February

covid_countydeaths_21 = wa_chd_21_deaths.copy()

covid_countydeaths_21_primary_key_list = [item for item in range(2962,3001)]

covid_countydeaths_21['CountyDeaths_ID'] = covid_countydeaths_21_primary_key_list

covid_countydeaths_21_merged = covid_countydeaths_21.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countydeaths_21_merged.rename({'Deaths':'CountyDeaths_Sum'},axis = 1,inplace = True)

covid_countydeaths_21_merged.columns

covid_countydeaths_21_merged = covid_countydeaths_21_merged[['CountyDeaths_ID','County_ID','CountyDeaths_Sum']]

covid_countydeaths_21_merged.loc[:,'LastUpdated'] = '2021-02-21'

covid_countydeaths = covid_countydeaths_7_merged.append(covid_countydeaths_21_merged)

covid_countydeaths = covid_countydeaths.sort_values(by='CountyDeaths_ID').reset_index(drop=True)


# In[34]:


### Covid County Hospitalizations on 7th March


covid_countyhospitalizations_7 = wa_chd_7_hospitalizations.copy()

covid_countyhospitalizations_7_primary_key_list = [item for item in range(30001,30040)]

covid_countyhospitalizations_7['CountyHospitalizations_ID'] = covid_countyhospitalizations_7_primary_key_list

covid_countyhospitalizations_7_merged = covid_countyhospitalizations_7.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countyhospitalizations_7_merged.columns

covid_countyhospitalizations_7_merged.rename({'Hospitalizations':'CountyHospitalizations_Sum'},axis = 1,inplace = True)

covid_countyhospitalizations_7_merged = covid_countyhospitalizations_7_merged[['CountyHospitalizations_ID','County_ID','CountyHospitalizations_Sum']]

covid_countyhospitalizations_7_merged.loc[:,'LastUpdated'] = '2021-03-07'


### Covid County Hospitalizations on 21st February

covid_countyhospitalizations_21 = wa_chd_21_hospitalizations.copy()

covid_countyhospitalizations_21_primary_key_list = [item for item in range(29962,30001)]

covid_countyhospitalizations_21['CountyHospitalizations_ID'] = covid_countyhospitalizations_21_primary_key_list

covid_countyhospitalizations_21_merged = covid_countyhospitalizations_21.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countyhospitalizations_21_merged.columns

covid_countyhospitalizations_21_merged.rename({'Hospitalizations':'CountyHospitalizations_Sum'},axis = 1,inplace = True)

covid_countyhospitalizations_21_merged = covid_countyhospitalizations_21_merged[['CountyHospitalizations_ID','County_ID','CountyHospitalizations_Sum']]

covid_countyhospitalizations_21_merged.loc[:,'LastUpdated'] = '2021-02-21'

covid_countyhospitalizations = covid_countyhospitalizations_7_merged.append(covid_countyhospitalizations_21_merged)

covid_countyhospitalizations = covid_countyhospitalizations.sort_values(by='CountyHospitalizations_ID').reset_index(drop=True)


# ## County Level Zip Data 

# In[35]:


wa_chd_zip_7_cases.head(5)


# In[36]:


### Covid Zip Deaths on 7th March 

covid_countycases_zip_7 = wa_chd_zip_7_cases.copy()

covid_countycases_zip_7.dtypes

zip_table.dtypes

covid_countycases_zip_7['Location_Name'] = covid_countycases_zip_7['Location_Name'].astype('int')

covid_countycases_zip_7_primary_key_list = [item for item in range(300,384)]

covid_countycases_zip_7['ZIPCases_ID'] = covid_countycases_zip_7_primary_key_list

covid_countycases_zip_7_merged = covid_countycases_zip_7.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countycases_zip_7_merged.columns

covid_countycases_zip_7_merged.rename({'Positives':'ZIPCases_Sum'},axis = 1,inplace = True)

covid_countycases_zip_7_merged = covid_countycases_zip_7_merged[['ZIPCases_ID','ZIP_ID','ZIPCases_Sum']]

covid_countycases_zip_7_merged['LastUpdated'] = '2021-03-07'

### Covid Zip Deaths on 21st February 

covid_countycases_zip_21 = wa_chd_zip_21_cases.copy()

covid_countycases_zip_21['Location_Name'] = covid_countycases_zip_21['Location_Name'].astype('int')

covid_countycases_zip_21_primary_key_list = [item for item in range(216,300)]

covid_countycases_zip_21['ZIPCases_ID'] = covid_countycases_zip_21_primary_key_list

covid_countycases_zip_21_merged = covid_countycases_zip_21.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countycases_zip_21_merged.columns

covid_countycases_zip_21_merged.rename({'Positives':'ZIPCases_Sum'},axis = 1,inplace = True)

covid_countycases_zip_21_merged = covid_countycases_zip_21_merged[['ZIPCases_ID','ZIP_ID','ZIPCases_Sum']]

covid_countycases_zip_21_merged['LastUpdated'] = '2021-02-21'

covid_zipcases = covid_countycases_zip_7_merged.append(covid_countycases_zip_21_merged)

covid_zipcases = covid_zipcases.sort_values(by='ZIPCases_ID').reset_index(drop=True)


# In[37]:


### Covid Zip Deaths on 7th March 

covid_countydeaths_zip_7 = wa_chd_zip_7_deaths.copy()

covid_countydeaths_zip_7['Location_Name'] = covid_countydeaths_zip_7['Location_Name'].astype('int')

covid_countydeaths_zip_7_primary_key_list = [item for item in range(3000,3084)]

covid_countydeaths_zip_7['ZIPDeaths_ID'] = covid_countydeaths_zip_7_primary_key_list

covid_countydeaths_zip_7_merged = covid_countydeaths_zip_7.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countydeaths_zip_7_merged.rename({'Deaths':'ZIPDeaths_Sum'},axis = 1,inplace = True)

covid_countydeaths_zip_7_merged = covid_countydeaths_zip_7_merged[['ZIPDeaths_ID','ZIP_ID','ZIPDeaths_Sum']]

covid_countydeaths_zip_7_merged.loc[:,'LastUpdated'] = '2021-03-07'

# -----------------

### Covid Zip Deaths on 21st February

covid_countydeaths_zip_21 = wa_chd_zip_21_deaths.copy()

covid_countydeaths_zip_21['Location_Name'] = covid_countydeaths_zip_21['Location_Name'].astype('int')

covid_countydeaths_zip_21_primary_key_list = [item for item in range(2916,3000)]

covid_countydeaths_zip_21['ZIPDeaths_ID'] = covid_countydeaths_zip_21_primary_key_list

covid_countydeaths_zip_21_merged = covid_countydeaths_zip_21.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countydeaths_zip_21_merged.rename({'Deaths':'ZIPDeaths_Sum'},axis = 1,inplace = True)

covid_countydeaths_zip_21_merged = covid_countydeaths_zip_21_merged[['ZIPDeaths_ID','ZIP_ID','ZIPDeaths_Sum']]

covid_countydeaths_zip_21_merged.loc[:,'LastUpdated'] = '2021-02-21'

covid_zipdeaths = covid_countydeaths_zip_7_merged.append(covid_countydeaths_zip_21_merged)

covid_zipdeaths = covid_zipdeaths.sort_values(by='ZIPDeaths_ID').reset_index(drop=True)


# In[38]:


### Covid Zip Hospitalizations on 7th March 


covid_countyhospitalizations_zip_7 = wa_chd_zip_7_hospitalizations.copy()

covid_countyhospitalizations_zip_7.dtypes

covid_countyhospitalizations_zip_7['Location_Name'] = covid_countyhospitalizations_zip_7['Location_Name'].astype('int')

covid_countyhospitalizations_zip_7_primary_key_list = [item for item in range(30000,30084)]

covid_countyhospitalizations_zip_7['ZIPHospitalizations_ID'] = covid_countyhospitalizations_zip_7_primary_key_list

covid_countyhospitalizations_zip_7_merged =covid_countyhospitalizations_zip_7.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countyhospitalizations_zip_7_merged  = covid_countyhospitalizations_zip_7_merged.rename({'Hospitalizations':'ZIPHospitalizations_Sum'},axis =1)

covid_countyhospitalizations_zip_7_merged = covid_countyhospitalizations_zip_7_merged[['ZIPHospitalizations_ID','ZIP_ID','ZIPHospitalizations_Sum']]

covid_countyhospitalizations_zip_7_merged.loc[:,'LastUpdated'] = '2021-03-07' 

# ----------------

### Covid Zip Hospitalizations on 21st February

covid_countyhospitalizations_zip_21 = wa_chd_zip_21_hospitalizations.copy()

covid_countyhospitalizations_zip_21['Location_Name'] = covid_countyhospitalizations_zip_21['Location_Name'].astype('int')

covid_countyhospitalizations_zip_21_primary_key_list = [item for item in range(29916,30000)]

covid_countyhospitalizations_zip_21['ZIPHospitalizations_ID'] = covid_countyhospitalizations_zip_21_primary_key_list

covid_countyhospitalizations_zip_21_merged =covid_countyhospitalizations_zip_21.merge(zip_table, left_on='Location_Name', right_on='ZIP_Code')

covid_countyhospitalizations_zip_21_merged  = covid_countyhospitalizations_zip_21_merged.rename({'Hospitalizations':'ZIPHospitalizations_Sum'},axis =1)

covid_countyhospitalizations_zip_21_merged = covid_countyhospitalizations_zip_21_merged[['ZIPHospitalizations_ID','ZIP_ID','ZIPHospitalizations_Sum']]

covid_countyhospitalizations_zip_21_merged.loc[:,'LastUpdated'] = '2021-02-21' 

covid_ziphospitalizations = covid_countyhospitalizations_zip_7_merged.append(covid_countyhospitalizations_zip_21_merged)

covid_ziphospitalizations = covid_ziphospitalizations.sort_values(by='ZIPHospitalizations_ID').reset_index(drop=True)


# ## Covid 19 Vaccinations County Wise

# In[39]:


### County Vaccinations on 7th March 


covid_countyvaccinations_7 = wa_vacc_7.copy()

covid_countyvaccinations_7_primary_key_list = [item for item in range(800,839)]

covid_countyvaccinations_7['CountyVaccinations_ID'] = covid_countyvaccinations_7_primary_key_list

covid_countyvaccinations_7_merged = covid_countyvaccinations_7.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countyvaccinations_7_merged = covid_countyvaccinations_7_merged.rename({'People Initiating Vaccination':'CountyVaccinations_Sum'},axis =1 )

covid_countyvaccinations_7_merged.columns

covid_countyvaccinations_7_merged = covid_countyvaccinations_7_merged[['CountyVaccinations_ID',
                                                                      'County_ID',
                                                                      'CountyVaccinations_Sum']]
                                                                      

covid_countyvaccinations_7_merged['LastUpdated'] = '2021-03-07'


### County Vaccinations on 21st February


covid_countyvaccinations_21 = wa_vacc_21.copy()

covid_countyvaccinations_21_primary_key_list = [item for item in range(761,800)]

covid_countyvaccinations_21['CountyVaccinations_ID'] = covid_countyvaccinations_21_primary_key_list

covid_countyvaccinations_21_merged = covid_countyvaccinations_21.merge(df_county_name, left_on='County', right_on='County_Name')

covid_countyvaccinations_21_merged = covid_countyvaccinations_21_merged.rename({'People Initiating Vaccination':'CountyVaccinations_Sum'},axis =1 )

covid_countyvaccinations_21_merged.columns

covid_countyvaccinations_21_merged = covid_countyvaccinations_21_merged[['CountyVaccinations_ID',
                                                                      'County_ID',
                                                                      'CountyVaccinations_Sum']]
                                                                      

covid_countyvaccinations_21_merged['LastUpdated'] = '2021-02-21'

covid_countyvaccinations = covid_countyvaccinations_7_merged.append(covid_countyvaccinations_21_merged)

covid_countyvaccinations = covid_countyvaccinations.sort_values(by='CountyVaccinations_ID').reset_index(drop=True)


# ## Vaccination Sites

# In[40]:


vaccination_sites_df = pd.read_excel(r'/Users/rohan20k/Desktop/data_imt_563/WaVaccinationSites__20210121181237.xlsx')

vaccination_sites_df.columns

'County','Facility','Address','ZIP','Info Website','Email','Phone','Scheduling Site','Instructions for Public','Walk-In Instructions','Appt Available'	

vaccination_sites_df = vaccination_sites_df[['County',
                                            'Facility','Address','ZIP','Info Website','Email','Phone','Scheduling Site','Instructions for Public','Walk-In Instructions','Appt Available']]

vaccination_sites_df.loc[:,'County'] = vaccination_sites_df.loc[:,'County'].apply(lambda x:x+' County') 

vaccination_sites_df_primary_key_list = [item for item in range(6000,6246)]

vaccination_sites_df['Site_ID'] = vaccination_sites_df_primary_key_list

vaccination_sites_df_merged =vaccination_sites_df.merge(zip_table, left_on='ZIP', right_on='ZIP_Code')

vaccination_sites_df_merged = vaccination_sites_df_merged[['Site_ID',
                                                          'County_ID',
                                                          'ZIP_ID',
                                                          'Facility',
                                                          'Address',
                                                          'Info Website',
                                                          'Email',
                                                          'Phone',
                                                          'Instructions for Public',
                                                           'Scheduling Site',
                                                           'Appt Available']] 

vaccination_sites_df_merged.loc[:,'LastUpdated'] = '2021-02-21'

vaccination_sites_df_merged = vaccination_sites_df_merged.rename({'Facility':'Site_Name',
                                                                 'Address':'Site_Address',
                                                                 'Info Website':'Info_URL',
                                                                  'Email':'Site_Email',
                                                                  'Phone':'Site_Phone',
                                                                   'Instructions for Public':'Site_Instructions',
                                                                  'Scheduling Site':'Scheduling_URL',
                                                                 'Appt Available':'Appt_Available_Status'},axis = 1)

vaccination_sites_df_merged['LastUpdated'] = vaccination_sites_df_merged['LastUpdated'].astype('datetime64[ns]') 

vaccination_sites_df_merged['Appt_Available_Status'] = vaccination_sites_df_merged['Appt_Available_Status'].astype('bool') 

vaccination_sites_df_merged.dtypes

vaccination_sites = vaccination_sites_df_merged.copy()


# ## Datatype Verification 

# state_name,
# county_name,
# zip_table,
# covid_countycases, 
# covid_countydeaths,
# covid_countyhospitalizations,
# covid_countyvaccinations,
# covid_zipcases, 
# covid_zipdeaths,
# covid_ziphospitalizations

# In[41]:


state_name.dtypes
state_name['State_ID'] = df_state_name['State_ID'].astype('int64')

state_name.dtypes

county_name.dtypes

zip_table.dtypes

covid_countycases['LastUpdated']=covid_countycases['LastUpdated'].astype('datetime64[ns]')
covid_countycases.dtypes


covid_countydeaths['LastUpdated']=covid_countydeaths['LastUpdated'].astype('datetime64[ns]')
covid_countydeaths.dtypes

covid_countyhospitalizations['LastUpdated']=covid_countyhospitalizations['LastUpdated'].astype('datetime64[ns]')
covid_countyhospitalizations.dtypes

covid_countyvaccinations['LastUpdated']=covid_countyvaccinations['LastUpdated'].astype('datetime64[ns]')
covid_countyvaccinations.dtypes

covid_zipcases['LastUpdated']=covid_zipcases['LastUpdated'].astype('datetime64[ns]')
covid_zipcases.dtypes

covid_zipdeaths['LastUpdated']=covid_zipdeaths['LastUpdated'].astype('datetime64[ns]')
covid_zipdeaths.dtypes

covid_ziphospitalizations['LastUpdated']=covid_ziphospitalizations['LastUpdated'].astype('datetime64[ns]')
covid_ziphospitalizations.dtypes

engine = create_engine(URL(
                            account = 'tca69088',
                            role = 'SYSADMIN',
                            user = 'Group7',
                            password = 'Jrv12345!',
                            database = 'IMT_DB',
                            schema = 'PUBLIC',
                            ))

def data_push(df,table_name):
    df['Upload_Timestamp'] = pd.Timestamp.now(tz="America/Los_Angeles")
    df.to_sql(table_name.lower(),con=engine,if_exists='replace',index = False)
    connection = engine.connect()
    connection.close()
    engine.dispose()
    print(f"Data has been successfully transferred to Snowflake in {table_name}")

pd.set_option("display.max_rows", None, "display.max_columns", None)


# ## Google Maps API 🗺 

# In[44]:


from googlemaps import Client as GoogleMaps

vaccination_sites_google = vaccination_sites.copy()

gmaps = GoogleMaps('AIzaSyCtZiB9OREwAE0_oni8FMb-RendpG5So_I')

vaccination_sites_google['lat'] = ''
vaccination_sites_google['long'] = ''

for x in tqdm(range(len(vaccination_sites_google))):
    try:
        geocode_result = gmaps.geocode(vaccination_sites_google['Site_Address'][x])
        vaccination_sites_google['lat'][x] = geocode_result[0]['geometry']['location'] ['lat']
        vaccination_sites_google['long'][x] = geocode_result[0]['geometry']['location']['lng']
    except IndexError:
        print("Address was wrong...")
    except Exception as e:
        print("Unexpected error occurred.", e )


# ## SEND TO SNOWFLAKE 🚀

# In[45]:


# table_name = "STATE_NAME"
# data_push(state_name,table_name)

# table_name = "COUNTY_NAME"
# data_push(county_name,table_name)

# table_name = "ZIP_TABLE"
# data_push(zip_table,table_name)

# table_name = "COVID_COUNTYCASES"
# data_push(covid_countycases,table_name)

# table_name = "COVID_COUNTYDEATHS"
# data_push(covid_countydeaths,table_name)

# table_name = "COVID_COUNTYHOSPITALIZATIONS"
# data_push(covid_countyhospitalizations,table_name)

# table_name = "COVID_COUNTYVACCINATIONS"
# data_push(covid_countyvaccinations,table_name)

# table_name = "VACCINATION_SITES"
# data_push(vaccination_sites_google,table_name)

# table_name = "COVID_ZIPCASES"
# data_push(covid_zipcases,table_name)

# table_name = "COVID_ZIPDEATHS"
# data_push(covid_zipdeaths,table_name)

# table_name = "COVID_ZIPHOSPITALIZATIONS"
# data_push(covid_ziphospitalizations,table_name)

