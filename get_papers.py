import dask.dataframe as dd
import pandas as pd
import csv

papers_file = 'Papers.txt'
AI_conferences_file = "AIConferenceSeries.txt"
inter_dis_conferences_file = "InterDisConferenceSeries.txt"
systems_conferences_file = "SystemsConferenceSeries.txt"
theory_conferences_file = "TheoryConferenceSeries.txt"

paper_cols = ['PaperId', 'Rank', 'Doi', 'DocType', 'PaperTitle', 
         'OriginalTitle', 'BookTitle', 'Year', 'Date', 
         'OnlineDate', 'Publisher', 'JournalId', 'ConferenceSeriesId', 
         'ConferenceInstanceId', 'Volume', 'Issue', 'FirstPage', 'LastPage', 
         'ReferenceCount', 'CitationCount', 'EstimatedCitation', 'OriginalVenue', 
         'FamilyId', 'FamilyRank', 'DocSubTypes', 'CreatedDate']

conference_cols= ["ConferenceSeriesId", "Rank", "NormalizedName",
	"DisplayName", "PaperCount", "PaperFamilyCount", 
	"CitationCount", "CreatedDate"]


conferences = pd.read_csv(AI_conferences_file, delimiter='\t', names=conference_cols)
AI_conferences_ids = set(conferences["ConferenceSeriesId"])

conferences = pd.read_csv(inter_dis_conferences_file, delimiter='\t', names=conference_cols)
inter_dis_conferences_ids = set(conferences["ConferenceSeriesId"])

conferences = pd.read_csv(theory_conferences_file, delimiter='\t', names=conference_cols)
theory_conferences_ids = set(conferences["ConferenceSeriesId"])

conferences = pd.read_csv(systems_conferences_file, delimiter='\t', names=conference_cols)
systems_conferences_ids = set(conferences["ConferenceSeriesId"])

needed_papers_cols = ['PaperId', 'PaperTitle', 'Year', 'Date', 'ConferenceSeriesId', 'ReferenceCount', 'CitationCount', 'FamilyId']

papers = dd.read_csv(papers_file, delimiter='\t', names=paper_cols, dtype={'DocSubTypes': 'object','FirstPage': 'object','LastPage':'object','Year': 'float64','CitationCount': 'float64','EstimatedCitation':'object','ReferenceCount': 'float64'},on_bad_lines='warn',quoting=csv.QUOTE_NONE)[needed_papers_cols]

df = papers[papers["ConferenceSeriesId"].isin(AI_conferences_ids)]
df = df[df["FamilyId"].isnull() | (df["FamilyId"]==df["PaperId"])]
df.to_csv("AIPapers.csv", index=False, single_file=True)

df = papers[papers["ConferenceSeriesId"].isin(inter_dis_conferences_ids)]
df = df[df["FamilyId"].isnull() | (df["FamilyId"]==df["PaperId"])]
df.to_csv("InterDisPapers.csv", index=False, single_file=True)

df = papers[papers["ConferenceSeriesId"].isin(theory_conferences_ids)]
df = df[df["FamilyId"].isnull() | (df["FamilyId"]==df["PaperId"])]
df.to_csv("TheoryPapers.csv", index=False, single_file=True)

df = papers[papers["ConferenceSeriesId"].isin(systems_conferences_ids)]
df = df[df["FamilyId"].isnull() | (df["FamilyId"]==df["PaperId"])]
df.to_csv("SystemsPapers.csv", index=False, single_file=True)
