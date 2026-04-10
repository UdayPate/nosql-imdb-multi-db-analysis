from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import os

# ── Connection ────────────────────────────────────────────────────────
URI      = "neo4j+s://36a5d866.databases.neo4j.io"
USERNAME = "36a5d866"
PASSWORD = "cVAAMtM61SOZcUXtCNR8Hv7slad-nGutgNMeg2OfDtY"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
output_dir = "../output"
os.makedirs(output_dir, exist_ok=True)

print("=" * 60)
print("IMDb Top 1000 - Neo4j Graph Analytics")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────
# QUERY 1: Most Prolific Actors (most movies in top 1000)
# ─────────────────────────────────────────────────────────────────────
print("\n Query 1: Top 10 Most Prolific Actors")

with driver.session() as session:
    result = session.run("""
        MATCH (a:Actor)-[:ACTED_IN]->(m:Movie)
        RETURN a.name AS actor, count(m) AS movie_count
        ORDER BY movie_count DESC
        LIMIT 10
    """)
    q1 = [r.data() for r in result]

for r in q1:
    print(f"  {r['actor']:<25} {r['movie_count']} movies")

# Plot
actors = [r["actor"].split(" ")[-1] for r in q1]
counts = [r["movie_count"] for r in q1]
plt.figure(figsize=(10, 5))
plt.barh(actors[::-1], counts[::-1], color="steelblue")
plt.title("Top 10 Most Prolific Actors in IMDb Top 1000")
plt.xlabel("Number of Movies")
plt.tight_layout()
plt.savefig(f"{output_dir}/neo4j_q1_prolific_actors.png")
plt.close()
print("Chart saved: output/neo4j_q1_prolific_actors.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 2: Actor Collaborations (pairs who worked together most)
# ─────────────────────────────────────────────────────────────────────
print("\n Query 2: Top 10 Actor Pairs Who Collaborated Most")

with driver.session() as session:
    result = session.run("""
        MATCH (a1:Actor)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(a2:Actor)
        WHERE a1.name < a2.name
        RETURN a1.name AS actor1, a2.name AS actor2, 
               count(m) AS collaborations,
               collect(m.title)[0..3] AS sample_movies
        ORDER BY collaborations DESC
        LIMIT 10
    """)
    q2 = [r.data() for r in result]

for r in q2:
    print(f"  {r['actor1']} & {r['actor2']}")
    print(f"    → {r['collaborations']} movies together: {r['sample_movies']}")

# ─────────────────────────────────────────────────────────────────────
# QUERY 3: Directors who worked with the most unique actors
# ─────────────────────────────────────────────────────────────────────
print("\n Query 3: Directors with Most Unique Actors")

with driver.session() as session:
    result = session.run("""
        MATCH (d:Director)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Actor)
        RETURN d.name AS director, 
               count(DISTINCT a) AS unique_actors,
               count(DISTINCT m) AS movies_directed
        ORDER BY unique_actors DESC
        LIMIT 10
    """)
    q3 = [r.data() for r in result]

for r in q3:
    print(f"  {r['director']:<25} {r['unique_actors']} unique actors across {r['movies_directed']} movies")

# Plot
directors = [r["director"].split(" ")[-1] for r in q3]
unique_actors = [r["unique_actors"] for r in q3]
plt.figure(figsize=(10, 5))
plt.barh(directors[::-1], unique_actors[::-1], color="darkorange")
plt.title("Directors with Most Unique Actors Worked With")
plt.xlabel("Number of Unique Actors")
plt.tight_layout()
plt.savefig(f"{output_dir}/neo4j_q3_directors_actors.png")
plt.close()
print("Chart saved: output/neo4j_q3_directors_actors.png")

# ─────────────────────────────────────────────────────────────────────
# QUERY 4: Six Degrees of Separation between two actors
# Find shortest path connecting any two actors through movies
# ─────────────────────────────────────────────────────────────────────
print("\n Query 4: Six Degrees of Separation")
print("  Finding shortest path: Tom Hanks → Leonardo DiCaprio")

with driver.session() as session:
    result = session.run("""
        MATCH path = shortestPath(
            (a1:Actor {name: 'Tom Hanks'})-[*]-(a2:Actor {name: 'Leonardo DiCaprio'})
        )
        RETURN [node in nodes(path) | 
                CASE 
                    WHEN 'Actor' IN labels(node) THEN 'Actor: ' + node.name
                    WHEN 'Movie' IN labels(node) THEN 'Movie: ' + node.title
                    ELSE node.name 
                END
               ] AS path_nodes,
               length(path) AS degrees
    """)
    q4 = [r.data() for r in result]

if q4:
    print(f"  Degrees of separation: {q4[0]['degrees'] // 2}")
    print("  Path:")
    for node in q4[0]["path_nodes"]:
        print(f"    → {node}")
else:
    print("  No direct path found between these actors")

# ─────────────────────────────────────────────────────────────────────
# QUERY 5: Highest rated movies per genre (graph traversal)
# ─────────────────────────────────────────────────────────────────────
print("\n Query 5: Highest Rated Movie per Genre")

with driver.session() as session:
    result = session.run("""
        MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
        WITH g, m ORDER BY m.rating DESC
        WITH g, collect(m)[0] AS top_movie
        RETURN g.name AS genre, 
               top_movie.title AS best_movie,
               top_movie.rating AS rating
        ORDER BY rating DESC
        LIMIT 10
    """)
    q5 = [r.data() for r in result]

for r in q5:
    print(f"  {r['genre']:<15} {r['best_movie']:<40} ⭐ {r['rating']}")

print("\n" + "=" * 60)
print("All Neo4j queries complete!")
print("=" * 60)

driver.close()