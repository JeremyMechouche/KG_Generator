from neo4j import GraphDatabase 

def clear_database(url, user, password): 
    driver = GraphDatabase.driver(url, auth=(user, password)) 
    with driver.session() as session: 
        session.run("MATCH (n) DETACH DELETE n") 
    driver.close() 
    print("Database cleared!") 


if __name__ == "__main__": 
    url = "bolt://localhost:7687" 
    user = "neo4j" 
    password = "ceciestunM0tdepasse" 

    clear_database(url, user, password) 
