import pandas as pd
base_url = "https://data.pa.gov/resource/mcba-yywm.csv"
limit = 50000


def extract_data(base_url="https://data.pa.gov/resource/mcba-yywm.csv",limit=50000):
    offset = 0
    all_dfs = []
    
    while True:
        page_url = f"{base_url}?$limit={limit}&$offset={offset}"
        print(f"Retrieving rows from offset {offset}")
        
        try:
            df_chunk = pd.read_csv(page_url)
        except Exception as e:
            print(f"Error while fetching data at offset {offset}: {e}")
            break

        if df_chunk.empty:
            break

        all_dfs.append(df_chunk)
        offset += limit

    # Combining all chunks into one DataFrame
    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df

def to_snake_case(word):
    if pd.isnull(word):
        return word
    
    return ("_".join(word.lower().split(" ")))

def add_birth_year_column(df):
    df['dateofbirth'] = pd.to_datetime(df['dateofbirth'], errors='coerce')
    df['yr_born'] = df['dateofbirth'].dt.year.astype('Int64')
    cols = list(df.columns) # lists all the columns
    dob_index = cols.index('dateofbirth') # finds the index of dateofbirth column
    cols.insert(dob_index + 1, cols.pop(cols.index('yr_born'))) # pops the yr_born and places it beside the dob_index
    df = df[cols] # reorders the actual dataframe columns
    return df
    
def age_in_2020(df):
    df['age_in_2020']=2020-df['yr_born']
    df = df[(df['age_in_2020'] >= 18) & (df['age_in_2020'] <= 110)]
    print("Calculated ages of the voters and filtered out unrealistic ages (Ages below 18 and above 110)")
    return df

def age_party_relationship(df):
    print("ANALYSIS: Age and Party VS votes by mail requests")
    
    #Initial stats
    print("INITIAL STATISTICS")
    print(f"Total_mail_reqs: {len(df)}")
    print(f"Age range: {df['age_in_2020'].min()} to {df['age_in_2020'].max()} years")
    
    # party
    print("PARTY DESIGNATIONS")
    party_counts = df['party'].value_counts()
    party_percentages = df['party'].value_counts(normalize=True) * 100
    for party,count in party_counts.items():
        percentage = party_percentages[party]
        print(f"{party}: {count:,} ({percentage:.1f}%)")
    print()    
        
    #Age
    print("AGE BREAKDOWN")
    df['age_group'] = pd.cut(df['age_in_2020'], 
                            bins=[18,30,40,50,60,70,80,110], 
                            labels=['18-29','30-39','40-49','50-59','60-69','70-79','80+'],
                            right=False)
    age_counts = df['age_group'].value_counts().sort_index()
    age_percentages = df['age_group'].value_counts(normalize=True).sort_index() * 100
    for age_group,count in age_counts.items():
        percentage = age_percentages[age_group]
        print(f"{age_group}: {count:,} ({percentage:.1f}%)")
    print()
    
    # Age group by Party -- Count
    print("AGE GROUP BY PARTY")
    crosstab = pd.crosstab(df['age_group'],df['party'],margins=True)
    print(crosstab)
    print()
    
    # Age group by Party -- Percentage
    print("AGE GROUP BY PARTY in %")
    crosstab_pct = pd.crosstab(df['age_group'],df['party'],normalize='index')*100
    print(crosstab_pct.round(1))
    print()
    
    # 5. avg age by party
    print("AVERAGE AGE BY PARTY:")
    avg_age_by_party = df.groupby('party')['age_in_2020'].agg(['mean','median','std']).round(1)
    print(avg_age_by_party)
    print()
    
    # Key findings
    print("KEY INSIGHTS:")
    
    # party with highest mail ballot usage
    top_party = party_counts.iloc[0]
    top_party_pct = party_percentages.iloc[0]
    
    # age group with highest mail ballot usage
    top_age_group = age_counts.iloc[0]
    top_age_pct = age_percentages.iloc[0]
    
    # party that has highest/lowest avg age 
    oldest_party = avg_age_by_party['mean'].idxmax()
    youngest_party = avg_age_by_party['mean'].idxmin()
    
    print(f"-> {top_party} had the most mail ballot requests ({top_party_pct:.1f}% of total)")
    print(f"-> Age group {top_age_group} had the most requests ({top_age_pct:.1f}% of total)")
    print(f"-> {oldest_party} voters had the highest average age ({avg_age_by_party.loc[oldest_party,'mean']:.1f} years)")
    print(f"-> {youngest_party} voters had the lowest average age ({avg_age_by_party.loc[youngest_party,'mean']:.1f} years)")
    
    # Age > 65 (retirement age)
    older_voters_pct = ((df['age_in_2020'] >= 65).sum()/len(df))*100
    print(f"-> {older_voters_pct:.1f}% of mail ballot requests came from voters 65 and older")
    
    return df

def median_latency_by_district(df):
    # covert the dates into datetime format
    df['appissuedate'] = pd.to_datetime(df['appissuedate'],errors='coerce')
    df['ballotreturneddate'] = pd.to_datetime(df['ballotreturneddate'],errors='coerce')
    
    #latency in days
    df['latency_days'] = (df['ballotreturneddate']-df['appissuedate']).dt.days
    
    # Filter out rows with negative latency
    valid_data = df[(df['latency_days'] >= 0) & (df['latency_days'].notnull())]
    
    print(f"Records with valid application and return dates: {len(valid_data):,}")
    print(f"Records excluded due to missing/invalid dates: {len(df)-len(valid_data):,}")
    print()
    
    # median latency by legislative district
    latency_by_district = valid_data.groupby('legislative')['latency_days'].agg(['median','count']).round(1)
    latency_by_district.columns = ['Median_Days','Ballot_Count']
    
    #sort
    latency_by_district = latency_by_district.sort_values('Median_Days')
    
    print("MEDIAN LATENCY BY LEGISLATIVE DISTRICT (sorted by median):")
    print(f"{'District':<15} {'Median Days':<15}{'Count'}:<15")
    
    return df

def congressional_district_frequency(df):
    
    print("CONGRESSIONAL DISTRICT WITH HIGHEST BALLOT REQUEST FREQUENCY")
    
    # Count of ballot requests by congressional district
    congressional_counts = df['congressional'].value_counts()
    congressional_percentages = df['congressional'].value_counts(normalize=True) * 100
    
    print(f"Total congressional districts: {len(congressional_counts)}")
    print(f"Total ballot requests analyzed: {len(df):,}")
    print()
    
    print("TOP 5 CONGRESSIONAL DISTRICTS BY BALLOT REQUEST FREQUENCY:")
    print(f"{'District':<15} {'Median Days':<15} {'Count':<15}")
    
    for rank,(district,count) in enumerate(congressional_counts.head().items(),1):
        percentage = congressional_percentages[district]
        print(f"{str(district):<15} {count:<12,} {percentage:<12.2f}% {rank:<8}")
    
    print()
    
    # Find the top district
    top_district = congressional_counts.index[0]
    top_count = congressional_counts.iloc[0]
    top_percentage = congressional_percentages.iloc[0]
    
    print(f"Congressional District {top_district} has the highest frequency of ballot requests")
    print(f"-> Total requests: {top_count:,}")
    print(f"-> Percentage of all requests: {top_percentage:.2f}%")
    print()
    
    return df
    

# 1. Data Extraction
application_in = extract_data(base_url="https://data.pa.gov/resource/mcba-yywm.csv",limit=50000)
print(f"Number of records: {application_in.shape}")
print("-"*50)

# 2. Storing invalid data
invalid_data = application_in[application_in.isnull().any(axis=1)]
application_in.dropna(inplace=True)
print("Stored the rows with null values in invalid_data")
print(f"Number of records that has Null values in atleast one column is {invalid_data.shape[0]}")
print("-"*50)

# 3. Convert Senate entries in to snake case
application_in["senate"] = application_in["senate"].apply(to_snake_case)
print("Converted all state senate district (senate) entries in application_in to snake case i.e from '29TH SENATORIAL DISTRICT'=>'29th_senatorial_district'")
print("-"*50)

# 4. Add new field born year
application_in = add_birth_year_column(application_in)
print("Added new column for birth year of voter, appears next to dateofbirth column")
print(application_in.head())
print("-"*50)

# 5. Age,Party Vs mail ballot requests
application_in = age_in_2020(application_in)
application_in = age_party_relationship(application_in)
print("-"*50)

# 6. Median latency by District
application_in = median_latency_by_district(application_in)
print("-"*50)

# 7. Congressional district with highest frequency of ballot requests
application_in = congressional_district_frequency(application_in)
print("-"*50)

print("\nAll analyses completed successfully!")
