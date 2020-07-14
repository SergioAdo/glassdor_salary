# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 09:04:39 2020

@author: Serge Ado
"""

import pandas as pd
df = pd.read_csv('gd_data.csv')
df.shape
df.columns

#drop column Unnamed: 0
df = df.drop('Unnamed: 0', axis= 1)

# hourly salary and emp_provided salary
df['hourly'] = df['Salary Estimate'].apply(lambda x : 1 if 'per hour' in x.lower() else 0)
df['emp_provided'] = df['Salary Estimate'].apply(lambda x : 1 if 'employer provided salary' in x.lower() else 0)


#drop rows where Salary Estimate = '-1'
df = df[df['Salary Estimate'] != '-1']

#clean salary
salary= df['Salary Estimate'].apply(lambda x : x.split('(')[0])
salary_clean = salary.apply(lambda x : x.replace('$','').replace('K',''))
salary_clean = salary_clean.apply(lambda x: x.replace('Employer Provided Salary:', '').replace('Per Hour', ''))


#create avg salary
df['min_sal'] = salary_clean.apply(lambda x : x.split('-')[0]).astype('int')
df['max_sal'] = salary_clean.apply(lambda x : x.split('-')[1]).astype('int')
df['avg_sal'] = round((df['min_sal'] + df['max_sal'])/2)


#Remove ratings from company name (minus last 3 characters)
df['comp_name']= df.apply(lambda x : x['Company Name'] if x['Rating'] < 0 else x['Company Name'][:-3], axis= 1)

#State column
df['state']= df['Location'].apply(lambda x : x.split(',')[1])

#hq_state column
df['hq_state'] = df.apply(lambda x : 'unknown' if x['Headquarters']== '-1' else x['Headquarters'].split(',')[1], axis =1)

#same_state column
df['same_state']= df.apply(lambda x : 1 if x['state']== x['hq_state'] else 0 , axis=1)

#company age
df['company_age']= df.apply(lambda x : -1 if x['Founded']== -1 else 2020- x['Founded'], axis = 1)

#python column
df['python']= df['Job Description'].apply(lambda x : 1 if 'python' in x.lower() else 0)

#sql column
df['sql'] = df['Job Description'].apply(lambda x : 1 if 'sql' in x.lower() else 0)

#spark column
df['spark'] = df['Job Description'].apply(lambda x : 1 if 'spark' in x.lower() else 0)

#java column
df['java'] = df['Job Description'].apply(lambda x : 1 if 'java' in x.lower() else 0)

#tableau column
df['tableau'] = df['Job Description'].apply(lambda x : 1 if 'tableau' in x.lower() else 0)

#excel column
df['excel'] = df['Job Description'].apply(lambda x : 1 if 'excel' in x.lower() else 0)

#hadoop column
df['hadoop'] = df['Job Description'].apply(lambda x : 1 if 'hadoop' in x.lower() else 0)

#power bi column
df['powerbi'] = df['Job Description'].apply(lambda x : 1 if 'power bi' in x.lower() else 0)

#gcp column
df['gcp'] = df['Job Description'].apply(lambda x : 1 if 'gcp' in x.lower() else 0)

#aws column
df['aws']= df['Job Description'].apply(lambda x : 1 if 'aws' in x.lower() else 0)

#nosql column
df['nosql'] = df['Job Description'].apply(lambda x : 1 if 'nosql' in x.lower() else 0)

#mongodb column
df['mongo'] = df['Job Description'].apply(lambda x : 1 if 'mongodb' in x.lower() else 0)




#num_comp column
df['num_comp'] = df['Competitors'].apply(lambda x : 0 if x == '-1' else len(x.split(',')))

#Few functions

def title_simplifer(title):
    if 'data scientist' in title.lower():
        return 'data scientist'
    elif 'analyst' in title.lower():
        return 'data analyst'
    elif 'data engineer' in title.lower():
        return 'data engineer'
    elif 'manager' in title.lower():
        return 'manager'
    elif 'director' in title.lower():
        return 'director'
    elif 'machine learning' in title.lower():
        return 'ml'
    else:
        return 'na'
    
    
def seniority(title):
    if 'senior' in title.lower() or 'lead' in title.lower() or 'sr.' in title.lower() or 'sr' in title.lower() or 'principal' in title.lower():
        return 'senior'
    elif 'junior' in title.lower() or 'jr' in title.lower() or 'jr.' in title.lower():
        return 'junior'
    else:
        return 'na'


#create job_simp column
df['job_simp'] = df['Job Title'].apply(title_simplifer)

#create seniority column
df['seniority'] = df['Job Title'].apply(seniority)


#fix los angeles state
df['state']= df['state'].apply(lambda x : x.strip() if x.strip().lower() != 'los angeles' else 'CA')

# job description length
df['job_len']= df['Job Description'].apply(lambda x : len(x))

#hourly wage to yearly  (yearly = 2000* hourly)

df['min_sal']= df.apply(lambda x : x.min_sal*2 if x.hourly == 1 else x.min_sal, axis=1)
df['max_sal']= df.apply(lambda x : x.max_sal*2 if x.hourly == 1 else x.max_sal, axis=1)

#df[df['hourly']==1][['hourly', 'min_sal', 'max_sal']]
df['avg_sal'] = round((df['min_sal'] + df['max_sal'])/2)


df.to_csv('cleaned_dataset.csv', index= False)