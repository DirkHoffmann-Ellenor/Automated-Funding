import pandas as pd

df = pd.read_csv('health_wellbeing_results.csv')

print(df["organisation_website"].nunique())
print(df["organisation_website"].unique())

# keep only rows where organisation_website occurs exactly once
df = df[~df['organisation_website'].duplicated(keep=False)]

print(df["organisation_website"].nunique())
print(df["organisation_website"].unique())

df.to_csv('health_wellbeing_results_unique.csv', index=False)