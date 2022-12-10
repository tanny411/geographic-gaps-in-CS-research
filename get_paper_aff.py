import dask.dataframe as dd
import pandas as pd
import csv

paper_aff_file = "PaperAuthorAffiliations.txt"


AI_papers = pd.read_csv("AIPapers.csv")
systems_papers = pd.read_csv("SystemsPapers.csv")
inter_dis_papers = pd.read_csv("InterDisPapers.csv")
theory_papers = pd.read_csv("TheoryPapers.csv")

paper_aff_cols = ["PaperId", "AuthorId", "AffiliationId", "AuthorSequenceNumber", "OriginalAuthor", "OriginalAffiliation"]

needed_paper_aff_cols = ["PaperId", "AffiliationId"]

paper_aff = dd.read_csv(paper_aff_file, delimiter='\t', names=paper_aff_cols,on_bad_lines='warn',quoting=csv.QUOTE_NONE)[needed_paper_aff_cols]

df = paper_aff[paper_aff["PaperId"].isin(AI_papers["PaperId"])]
df.to_csv("AIPaperAff.csv", index=False, single_file=True)

df = paper_aff[paper_aff["PaperId"].isin(systems_papers["PaperId"])]
df.to_csv("SystemsPaperAff.csv", index=False, single_file=True)

df = paper_aff[paper_aff["PaperId"].isin(inter_dis_papers["PaperId"])]
df.to_csv("InterDisPaperAff.csv", index=False, single_file=True)

df = paper_aff[paper_aff["PaperId"].isin(theory_papers["PaperId"])]
df.to_csv("TheoryPaperAff.csv", index=False, single_file=True)
