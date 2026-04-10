import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import os

# ── Connect ───────────────────────────────────────────────────────────
CONNECTION_STRING = "mongodb+srv://admin:Ud%40y0803!!@moviescluster.zjnt68i.mongodb.net/?retryWrites=true&w=majority&appName=MoviesCluster"
client = MongoClient(CONNECTION_STRING)
collection = client["imdb"]["movies"]
output_dir = "../output"
os.makedirs(output_dir, exist_ok=True)

print("=" * 60)
print("IMDb Top 1000 - MongoDB Analytics")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────
# QUERY 1: Top 5 Genres by Average IMDb Rating
# Uses: $unwind (to flatten genre array), $group, $sort, $limit
# ─────────────────────────────────────────────────────────────────────
print("\nQuery 1: Top 5 Genres by Average IMDb Rating")

q1 = list(collection.aggregate([
    { "$unwind": "$Genre" },
    { "$group": {
        "_id": "$Genre",
        "avg_rating": { "$avg": "$IMDB_Rating" },
        "movie_count": { "$sum": 1 }
    }},
    { "$match": { "movie_count": { "$gte": 10 } } },  # only genres with 10+ movies
    { "$sort": { "avg_rating": -1 } },
    { "$limit": 5 }
]))

for r in q1:
    print(f"  {r['_id']:<20} Avg Rating: {r['avg_rating']:.2f}  ({r['movie_count']} movies)")

# Plot
genres = [r["_id"] for r in q1]
ratings = [r["avg_rating"] for r in q1]
plt.figure(figsize=(8, 5))
plt.bar(genres, ratings, color="steelblue")
plt.title("Top 5 Genres by Average IMDb Rating")
plt.ylabel("Average IMDb Rating")
plt.xlabel("Genre")
plt.ylim(7, 9)
plt.tight_layout()
plt.savefig(f"{output_dir}/q1_genre_ratings.png")
plt.close()
print("Chart saved: output/q1_genre_ratings.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 2: Top 10 Highest Grossing Directors
# Uses: $group, $sort, $limit, $match to filter nulls
# ─────────────────────────────────────────────────────────────────────
print("\n Query 2: Top 10 Highest Grossing Directors")

q2 = list(collection.aggregate([
    { "$match": { "Gross": { "$exists": True, "$ne": None } } },
    { "$group": {
        "_id": "$Director",
        "total_gross": { "$sum": "$Gross" },
        "movie_count": { "$sum": 1 },
        "avg_gross": { "$avg": "$Gross" }
    }},
    { "$sort": { "total_gross": -1 } },
    { "$limit": 10 }
]))

for r in q2:
    print(f"  {r['_id']:<25} Total: ${r['total_gross']:>15,.0f}  ({r['movie_count']} movies)")

# Plot
directors = [r["_id"].split(" ")[-1] for r in q2]  # last name only for readability
gross = [r["total_gross"] / 1e6 for r in q2]        # convert to millions
plt.figure(figsize=(10, 5))
plt.barh(directors[::-1], gross[::-1], color="darkorange")
plt.title("Top 10 Highest Grossing Directors (Total Box Office)")
plt.xlabel("Total Gross (Millions USD)")
plt.tight_layout()
plt.savefig(f"{output_dir}/q2_director_gross.png")
plt.close()
print("Chart saved: output/q2_director_gross.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 3: Average IMDb Rating by Decade
# Uses: $bucket to group years into decades
# ─────────────────────────────────────────────────────────────────────
print("\n Query 3: Average IMDb Rating by Decade")

q3 = list(collection.aggregate([
    { "$match": { "Released_Year": { "$exists": True, "$ne": None } } },
    { "$bucket": {
        "groupBy": "$Released_Year",
        "boundaries": [1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020, 2030],
        "default": "Other",
        "output": {
            "avg_rating": { "$avg": "$IMDB_Rating" },
            "movie_count": { "$sum": 1 }
        }
    }}
]))

for r in q3:
    if r["_id"] != "Other":
        print(f"  {r['_id']}s  Avg Rating: {r['avg_rating']:.2f}  ({r['movie_count']} movies)")

# Plot
decades = [str(r["_id"]) + "s" for r in q3 if r["_id"] != "Other"]
avg_ratings = [r["avg_rating"] for r in q3 if r["_id"] != "Other"]
plt.figure(figsize=(10, 5))
plt.plot(decades, avg_ratings, marker="o", color="green", linewidth=2)
plt.title("Average IMDb Rating by Decade")
plt.ylabel("Average IMDb Rating")
plt.xlabel("Decade")
plt.ylim(7, 9)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(f"{output_dir}/q3_rating_by_decade.png")
plt.close()
print(" Chart saved: output/q3_rating_by_decade.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 4: Movie Count by Genre
# Uses: $unwind, $group, $sort
# ─────────────────────────────────────────────────────────────────────
print("\n Query 4: Movie Count by Genre (Top 10)")

q4 = list(collection.aggregate([
    { "$unwind": "$Genre" },
    { "$group": {
        "_id": "$Genre",
        "count": { "$sum": 1 }
    }},
    { "$sort": { "count": -1 } },
    { "$limit": 10 }
]))

for r in q4:
    print(f"  {r['_id']:<20} {r['count']} movies")

# Plot
genre_names = [r["_id"] for r in q4]
counts = [r["count"] for r in q4]
plt.figure(figsize=(10, 5))
plt.bar(genre_names, counts, color="mediumpurple")
plt.title("Top 10 Genres by Movie Count in IMDb Top 1000")
plt.ylabel("Number of Movies")
plt.xlabel("Genre")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(f"{output_dir}/q4_genre_count.png")
plt.close()
print(" Chart saved: output/q4_genre_count.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 5: Runtime vs Rating Correlation
# Uses: $project, $match, $group to analyze runtime buckets
# ─────────────────────────────────────────────────────────────────────
print("\n Query 5: Does Runtime Affect IMDb Rating?")

q5 = list(collection.aggregate([
    { "$match": { "Runtime": { "$exists": True, "$ne": None } } },
    { "$bucket": {
        "groupBy": "$Runtime",
        "boundaries": [0, 90, 120, 150, 180, 300],
        "default": "Other",
        "output": {
            "avg_rating": { "$avg": "$IMDB_Rating" },
            "avg_votes": { "$avg": "$No_of_Votes" },
            "count": { "$sum": 1 }
        }
    }}
]))

labels = ["<90 min", "90-120 min", "120-150 min", "150-180 min", "180+ min"]
for i, r in enumerate(q5):
    if r["_id"] != "Other":
        print(f"  {labels[i]:<15} Avg Rating: {r['avg_rating']:.2f}  ({r['count']} movies)")

# Plot
avg_r = [r["avg_rating"] for r in q5 if r["_id"] != "Other"]
plt.figure(figsize=(8, 5))
plt.bar(labels, avg_r, color="tomato")
plt.title("Average IMDb Rating by Runtime Bucket")
plt.ylabel("Average IMDb Rating")
plt.xlabel("Runtime")
plt.ylim(7, 9)
plt.tight_layout()
plt.savefig(f"{output_dir}/q5_runtime_rating.png")
plt.close()
print("Chart saved: output/q5_runtime_rating.png")

# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("All queries complete! Charts saved to output/ folder.")
print("=" * 60)
client.close()