import threading
import time

from KnowledgeGraphConstructor import GraphConstructor
from KnowledgeGraphUpgrader import GraphUpgrader

from KubernetesSource import KubernetesSource
from AwsSource import AwsSource

def run_upgrader(url, user, password, interval=5):
    upgrader = GraphUpgrader(url, user, password)
    try:
        while True:
            upgrader.upgrade_nodes()
            upgrader.upgrade_pods()  
            print("Graph upgraded with latest Kubernetes metrics (all namespaces).")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping GraphUpgrader...")
    finally:
        upgrader.close()

def main():
    # Connection settings
    url = "bolt://localhost:7687"
    user = "neo4j"
    password = "ceciestunM0tdepasse"
    input_file = "config.yaml"

    aws_source = AwsSource()

    sources = [KubernetesSource(), aws_source]

    # --- Step 1: Create the initial graph ---
    print("Creating initial graph...")
    creator = GraphConstructor(url, user, password, input_file, data_sources=sources)
    creator.create_graph()
    creator.close()
    print("Graph created successfully!")

    # --- Step 2: Upgrade the graph with metrics ---
    print("Starting GraphUpgrader thread...")
    upgrader_thread = threading.Thread(
        target=run_upgrader,
        args=(url, user, password, 5),  # update every 5s
        daemon=True
    )
    upgrader_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Main stopped. Upgrader thread will exit.")


if __name__ == "__main__":
    main()
