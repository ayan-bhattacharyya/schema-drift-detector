import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")

def verify_data():
    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        print("Error: Missing Neo4j environment variables.")
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    pipeline = "CRM-To-Finance-PeopleData"
    query = """
    MATCH (ic:IntegrationCatalog {pipeline: $pipeline})
    RETURN ic
    """
    
    print(f"Connecting to {NEO4J_URI} (db: {NEO4J_DB})...")
    try:
        with driver.session(database=NEO4J_DB) as session:
            result = session.run(query, pipeline=pipeline)
            record = result.single()
            
            if record:
                print(f"SUCCESS: Found IntegrationCatalog for pipeline '{pipeline}'")
                print(record["ic"])
            else:
                print(f"FAILURE: No IntegrationCatalog found for pipeline '{pipeline}'")
                
            # Check total counts
            count_query = "MATCH (n) RETURN labels(n) as label, count(*) as count"
            print("\n--- Node Counts ---")
            res = session.run(count_query)
            for r in res:
                print(f"{r['label']}: {r['count']}")
                
    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    verify_data()
