
import pandas as pd

def process_files(social_network_file, job_change_journal_file, output_file, avg_output_file):
    # Load the SocialNetwork.tsv file
    
    social_network_df = pd.read_csv(social_network_file, delimiter='\t', on_bad_lines='skip', header=None, names=['time', 'from', 'to'])

    # Filtering for the last day starting from row 2
    social_network_last_day_df = social_network_df.iloc[2:]
    total_connections = social_network_last_day_df['from'].value_counts().reset_index()
    total_connections.columns = ['agentid', 'totalconnections']

    # Load the JobChangeJournal.tsv file
    job_change_journal_df = pd.read_csv(job_change_journal_file, delimiter='\t', on_bad_lines='skip', header=None, names=['step', 'agentId', '[job,workingHour]'])

    # Extracting working hours
    job_change_journal_df['workingHour'] = job_change_journal_df['[job,workingHour]'].str.extract(r',(\d+)\]$')
    job_change_journal_df['workingHour'] = job_change_journal_df['workingHour'].fillna('0').astype(int)

    # Keeping only the last job change for each agent
    last_job_change = job_change_journal_df.drop_duplicates(subset=['agentId'], keep='last')

    # Merging the data
    final_merged_df = pd.merge(last_job_change[['agentId', 'workingHour']], total_connections, left_on='agentId', right_on='agentid', how='outer')
    final_merged_df.drop('agentId', axis=1, inplace=True)

    # Saving the final merged data to a CSV file
    final_merged_df.to_csv(output_file, index=False)
    
    # Calculating the average total connections per working hour
    avg_connections_per_hour = final_merged_df.groupby('workingHour')['totalconnections'].mean().reset_index()
    avg_connections_per_hour.columns = ['workinghour', 'average_totalconnections']
    
    # Saving the average connections per working hour to a CSV file
    avg_connections_per_hour.to_csv(avg_output_file, index=False)

    print(f"Data processed and saved to {output_file}")
    print(f"Average connections per working hour saved to {avg_output_file}")

# Example usage
process_files(r'C:\Users\youse\OneDrive\Desktop\POL Study\Run0\3months\SocialNetwork.tsv', r'C:\Users\youse\OneDrive\Desktop\POL Study\Run0\3months\JobChangeJournal.tsv', r'C:\Users\youse\OneDrive\Desktop\POL Study\Run0\3months\finalCombinedData.csv', r'C:\Users\youse\OneDrive\Desktop\POL Study\Run0\3months\average connections per working hour.csv')
