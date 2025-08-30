import yaml
import json
from neo4j import GraphDatabase
from kubernetes import client, config

class GraphConstructor:
    def __init__(self, url, auth_id, auth_passwd, input_file, data_sources=None):
        self.driver = GraphDatabase.driver(url, auth=(auth_id, auth_passwd))
        self.data_sources = data_sources or []
        self.input_file = input_file

    def load_config(self):
        """Load Demand, Application, Service definitions from YAML/JSON"""
        if self.input_file.endswith(".yaml") or self.input_file.endswith(".yml"):
            with open(self.input_file, "r") as f:
                return yaml.safe_load(f)
        elif self.input_file.endswith(".json"):
            with open(self.input_file, "r") as f:
                return json.load(f)
        else:
            raise ValueError("Input file must be .yaml/.yml or .json")

    def create_graph(self):
        """Create nodes and relationships from input config + Kubernetes resources"""
        config_data = self.load_config()

        """ Iterate over all data sources and insert into Knowledge Graph """
        if not self.data_sources:
            print("No data sources defined. Exiting.")
            return

        def _create(tx): 
            # 1. Demands
            for demand in config_data.get("demands", []):
                tx.run("""
                    MERGE (d:Demand {id: $id})
                    SET d += $props
                """, id=demand["id"], props=demand)

                # Relation Demand -> Application
                if "application" in demand:
                    tx.run("""
                        MATCH (d:Demand {id: $did}),
                              (a:Application {id: $aid})
                        MERGE (d)-[:SOLVED_BY]->(a)
                    """, did=demand["id"], aid=demand["application"])

            # 2. Applications
            for app in config_data.get("applications", []):
                tx.run("""
                    MERGE (a:Application {id: $id})
                    SET a += $props
                """, id=app["id"], props=app)

                # Services associés
                for svc in app.get("services", []):
                    tx.run("""
                        MERGE (s:Service {id: $id})
                        SET s += $props
                    """, id=svc["id"], props=svc)

                    tx.run("""
                        MATCH (a:Application {id: $aid}),
                              (s:Service {id: $sid})
                        MERGE (a)-[:COMPOSED_OF]->(s)
                    """, aid=app["id"], sid=svc["id"])

                    # 3. Lier Services aux Ressources 
                    self.link_service_to_resources(tx, svc)

            for source in self.data_sources: 
                resources, relations = source.fetch_resources() 

                # Insert resources 
                for res in resources: 
                    tx.run(""" 
                        MERGE (r:Resource {id: $id}) 
                        SET r += $props 
                    """, id=res["id"], props=res) 

                # Insert relations 
                for rel in relations: 
                    tx.run(""" 
                        MATCH (a:Resource {id: $src}), 
                              (b:Resource {id: $dst}) 
                        MERGE (a)-[r:%s]->(b) 
                    """ % rel["type"], src=rel["src"], dst=rel["dst"]) 

        with self.driver.session() as session:
            session.write_transaction(_create)

    def link_service_to_resources(self, tx, service):
        """
        Link a Service node to existing Resource nodes in Neo4j
        based on matching labels (service=<service_name>).
        """
        # Recherche des ressources dont le label service correspond
        query = """
        MATCH (r:Resource)
        WHERE r.labels['service'] = $service_name
        MATCH (s:Service {id: $sid})
        MERGE (s)-[:RUNS_ON]->(r)
        """
        tx.run(query, service_name=service["name"], sid=service["id"])

    def close(self):
        self.driver.close()
