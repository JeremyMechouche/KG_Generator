import time
from neo4j import GraphDatabase
from kubernetes import client, config


class GraphUpgrader:
    def __init__(self, url, auth_id, auth_passwd, kube_config_path=None):
        self.driver = GraphDatabase.driver(url, auth=(auth_id, auth_passwd))

        # Connexion au cluster Kubernetes
        if kube_config_path:
            config.load_kube_config(config_file=kube_config_path)
        else:
            config.load_kube_config()  # charge ~/.kube/config par défaut

        self.core_v1 = client.CoreV1Api()
        self.metrics = client.CustomObjectsApi()

    def close(self):
        self.driver.close()

    # ---------------------
    #  PARSE METRICS K8S
    # ---------------------
    def parse_cpu(self, cpu_str):
        """Convertit CPU K8s en milliCPU"""
        if cpu_str.endswith("n"):  # nanocores
            return int(cpu_str[:-1]) / 1e6
        elif cpu_str.endswith("m"):  # millicores
            return int(cpu_str[:-1])
        else:  # cores
            return int(cpu_str) * 1000

    def parse_memory(self, mem_str):
        """Convertit mémoire K8s en Mi"""
        factors = {"Ki": 1/1024, "Mi": 1, "Gi": 1024}
        for unit, factor in factors.items():
            if mem_str.endswith(unit):
                return int(mem_str.replace(unit, "")) * factor
        return int(mem_str) / (1024*1024)

    # ---------------------
    #  COLLECT METRICS
    # ---------------------
    def collect_node_metrics(self):
        """Récupère les métriques CPU/memoire des nodes"""
        metrics = self.metrics.list_cluster_custom_object(
            group="metrics.k8s.io", version="v1beta1", plural="nodes"
        )
        results = {}
        for item in metrics["items"]:
            node_name = item["metadata"]["name"]
            cpu = self.parse_cpu(item["usage"]["cpu"])
            memory = self.parse_memory(item["usage"]["memory"])
            results[node_name] = {"cpuUsage": cpu, "memoryUsage": memory}
        return results

    def collect_pod_metrics(self):
        """Récupère les métriques CPU/memoire de tous les pods (tous namespaces)"""
        metrics = self.metrics.list_cluster_custom_object(
            group="metrics.k8s.io", version="v1beta1", plural="pods"
        )
        results = {}
        for item in metrics["items"]:
            pod_name = f"{item['metadata']['namespace']}/{item['metadata']['name']}"
            cpu_total = 0
            mem_total = 0
            for c in item["containers"]:
                cpu_total += self.parse_cpu(c["usage"]["cpu"])
                mem_total += self.parse_memory(c["usage"]["memory"])
            results[pod_name] = {"cpuUsage": cpu_total, "memoryUsage": mem_total}
        return results

    # ---------------------
    #  UPGRADE IN NEO4J
    # ---------------------
    def upgrade_nodes(self):
        node_metrics = self.collect_node_metrics()
        with self.driver.session() as session:
            for node_name, metrics in node_metrics.items():
                session.run(
                    """
                    MATCH (r:Resource {type:'Node', name:$name})
                    SET r += $metrics
                    """,
                    name=node_name,
                    metrics=metrics,
                )

    def upgrade_pods(self):
        """Met à jour tous les pods de tous les namespaces dans Neo4j"""
        pod_metrics = self.collect_pod_metrics()
        with self.driver.session() as session:
            for pod_name, metrics in pod_metrics.items():
                session.run(
                    """
                    MATCH (r:Resource {type:'Pod', name:$name})
                    SET r += $metrics
                    """,
                    name=pod_name,
                    metrics=metrics,
                )
