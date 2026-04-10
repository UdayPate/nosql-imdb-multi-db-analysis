import pandas as pd
from neo4j import GraphDatabase

# ── Connection Details ────────────────────────────────────────────────
URI      = "neo4j+s://36a5d866.databases.neo4j.io"
USERNAME = "36a5d866"
PASSWORD = "cVAAMtM61SOZcUXtCNR8Hv7slad-nGutgNMeg2OfDtY"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
driver.verify_connectivity()
print("Connected to Neo4j successfully!")

# ── Load CSV ──────────────────────────────────────────────────────────
df = pd.read_csv("../data/imdb_top_1000.csv")
df["Genre"] = df["Genre"].str.split(", ")
df["Gross"] = pd.to_numeric(df["Gross"].str.replace(",", "", regex=False), errors="coerce")
df["Released_Year"] = pd.to_numeric(df["Released_Year"], errors="coerce")
df["Runtime"] = df["Runtime"].str.replace(" min", "", regex=False).astype(float)

# ── Clear Existing Graph ──────────────────────────────────────────────
def clear_database(tx):
    tx.run("MATCH (n) DETACH DELETE n")

# ── Create Constraints ────────────────────────────────────────────────
def create_constraints(tx):
    tx.run("CREATE CONSTRAINT movie_title IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE")
    tx.run("CREATE CONSTRAINT actor_name IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE")
    tx.run("CREATE CONSTRAINT director_name IF NOT EXISTS FOR (d:Director) REQUIRE d.name IS UNIQUE")
    tx.run("CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")

# ── Create Movie Node ─────────────────────────────────────────────────
def create_movie(tx, row):
    tx.run("""
        MERGE (m:Movie {title: $title})
        SET m.year = $year,
            m.rating = $rating,
            m.runtime = $runtime,
            m.gross = $gross,
            m.meta_score = $meta_score,
            m.votes = $votes,
            m.overview = $overview
    """,
    title=row["Series_Title"],
    year=row["Released_Year"],
    rating=row["IMDB_Rating"],
    runtime=row["Runtime"],
    gross=row["Gross"],
    meta_score=row["Meta_score"],
    votes=row["No_of_Votes"],
    overview=row["Overview"])

# ── Create Director → Movie ───────────────────────────────────────────
def create_director(tx, row):
    tx.run("""
        MERGE (d:Director {name: $director})
        MERGE (m:Movie {title: $title})
        MERGE (d)-[:DIRECTED]->(m)
    """,
    director=row["Director"],
    title=row["Series_Title"])

# ── Create Actor → Movie ──────────────────────────────────────────────
def create_actors(tx, row):
    for star in ["Star1", "Star2", "Star3", "Star4"]:
        if pd.notna(row[star]):
            tx.run("""
                MERGE (a:Actor {name: $actor})
                MERGE (m:Movie {title: $title})
                MERGE (a)-[:ACTED_IN]->(m)
            """,
            actor=row[star],
            title=row["Series_Title"])

# ── Create Genre → Movie ──────────────────────────────────────────────
def create_genres(tx, row):
    if isinstance(row["Genre"], list):
        for genre in row["Genre"]:
            tx.run("""
                MERGE (g:Genre {name: $genre})
                MERGE (m:Movie {title: $title})
                MERGE (m)-[:IN_GENRE]->(g)
            """,
            genre=genre,
            title=row["Series_Title"])

# ── Run Everything ────────────────────────────────────────────────────
print("Clearing existing graph...")
with driver.session() as session:
    session.execute_write(clear_database)

print("Creating constraints...")
with driver.session() as session:
    session.execute_write(create_constraints)

print("Loading movies into Neo4j...")
total = len(df)

with driver.session() as session:
    for i, (_, row) in enumerate(df.iterrows()):
        session.execute_write(create_movie, row)
        session.execute_write(create_director, row)
        session.execute_write(create_actors, row)
        session.execute_write(create_genres, row)

        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{total} movies...")

print(f"\nDone! Loaded {total} movies into Neo4j!")
print("Nodes created: Movie, Actor, Director, Genre")
print("Relationships: ACTED_IN, DIRECTED, IN_GENRE")

driver.close()