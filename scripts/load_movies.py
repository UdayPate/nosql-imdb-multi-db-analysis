import pandas as pd
from pymongo import MongoClient

# MongoDB Atlas connection string
CONNECTION_STRING = "mongodb+srv://admin:Ud%40y0803!!@moviescluster.zjnt68i.mongodb.net/?retryWrites=true&w=majority&appName=MoviesCluster"

# Connect to MongoDB
client = MongoClient(CONNECTION_STRING)

# Test connection
print("Connected successfully!")
print("Existing databases:", client.list_database_names())

db = client["imdb"]
collection = db["movies"]

# Load CSV
df = pd.read_csv("../data/imdb_top_1000.csv")

# Clean Runtime: "142 min" -> 142
df["Runtime"] = df["Runtime"].str.replace(" min", "", regex=False).astype(float)

# Clean Gross: "28,341,469" -> 28341469
df["Gross"] = df["Gross"].str.replace(",", "", regex=False)
df["Gross"] = pd.to_numeric(df["Gross"], errors="coerce")

# Clean Released_Year
df["Released_Year"] = pd.to_numeric(df["Released_Year"], errors="coerce")

# Clean Meta_score
df["Meta_score"] = pd.to_numeric(df["Meta_score"], errors="coerce")

# Split Genre into list
df["Genre"] = df["Genre"].str.split(", ")

#combine cast into an array for easier MongoDB querying
df["Cast"] = df[["Star1", "Star2", "Star3", "Star4"]].values.tolist()

# Drop poster link it's not needed
df = df.drop(columns=["Poster_Link"])

# Convert to documents
documents = df.to_dict(orient="records")

# Insert into MongoDB
collection.drop()
result = collection.insert_many(documents)

print(f"Inserted {len(result.inserted_ids)} movies into MongoDB!")

client.close()