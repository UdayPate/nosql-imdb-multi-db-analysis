import redis
import json
import time
from pymongo import MongoClient

# ── Redis Connection ──────────────────────────────────────────────────
r = redis.Redis(
    host="redis-18599.c60.us-west-1-2.ec2.cloud.redislabs.com",
    port=18599,
    username="default",
    password="WS5nrwhf51kc2INdNGw6wUjLChNXjrMc",
    decode_responses=True
)

# ── MongoDB Connection ────────────────────────────────────────────────
MONGO_URI = "mongodb+srv://admin:Ud%40y0803!!@moviescluster.zjnt68i.mongodb.net/?retryWrites=true&w=majority&appName=MoviesCluster"
mongo = MongoClient(MONGO_URI)
collection = mongo["imdb"]["movies"]

print("=" * 60)
print("Redis Caching Demo - IMDb Movies")
print("=" * 60)

# ── Helper: Get movie with caching ───────────────────────────────────
def get_movie(title):
    cache_key = f"movie:{title.lower().replace(' ', '_')}"

    # 1. Check Redis cache first
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached), "CACHE HIT"

    # 2. If not cached, query MongoDB
    movie = collection.find_one({"Series_Title": title}, {"_id": 0})
    if movie:
        # Store in Redis with 1 hour expiry (3600 seconds)
        r.setex(cache_key, 3600, json.dumps(movie, default=str))
        return movie, "CACHE MISS (fetched from MongoDB)"

    return None, "NOT FOUND"

# ── Demo: Cache Performance Comparison ───────────────────────────────
test_movies = [
    "The Shawshank Redemption",
    "The Godfather",
    "The Dark Knight",
    "Inception",
    "Forrest Gump"
]

print("\n Round 1: First lookup (Cache MISS - fetching from MongoDB)")
print("-" * 60)
for title in test_movies:
    start = time.time()
    movie, status = get_movie(title)
    elapsed = (time.time() - start) * 1000
    if movie:
        print(f"  {title:<35} ⭐{movie['IMDB_Rating']}  {elapsed:.1f}ms  [{status}]")

print("\n Round 2: Second lookup (Cache HIT - fetching from Redis)")
print("-" * 60)
for title in test_movies:
    start = time.time()
    movie, status = get_movie(title)
    elapsed = (time.time() - start) * 1000
    if movie:
        print(f"  {title:<35} ⭐{movie['IMDB_Rating']}  {elapsed:.1f}ms  [{status}]")

# ── Store Top 10 leaderboard in Redis ────────────────────────────────
print("\n Storing Top 10 Movies Leaderboard in Redis...")
print("-" * 60)
top10 = list(collection.find(
    {}, {"Series_Title": 1, "IMDB_Rating": 1, "_id": 0}
).sort("IMDB_Rating", -1).limit(10))

# Use Redis Sorted Set for leaderboard
r.delete("leaderboard:top_movies")
for movie in top10:
    r.zadd("leaderboard:top_movies", {movie["Series_Title"]: movie["IMDB_Rating"]})

# Retrieve leaderboard
print("Top 10 Movies from Redis Leaderboard:")
leaderboard = r.zrevrange("leaderboard:top_movies", 0, -1, withscores=True)
for i, (title, score) in enumerate(leaderboard, 1):
    print(f"  {i:>2}. {title:<45} ⭐ {score}")

# ── Cache Stats ───────────────────────────────────────────────────────
print("\nRedis Cache Info:")
print("-" * 60)
info = r.info("stats")
print(f"  Total commands processed : {info['total_commands_processed']}")
print(f"  Keyspace hits            : {info['keyspace_hits']}")
print(f"  Keyspace misses          : {info['keyspace_misses']}")
keys = r.keys("movie:*")
print(f"  Cached movies            : {len(keys)}")

print("\n" + "=" * 60)
print(" Redis demo complete!")
print("=" * 60)

r.close()
mongo.close()