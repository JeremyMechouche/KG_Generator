from kubernetes import client, config

class KubernetesSource:
    def __init__(self):
        try:
            config.load_kube_config()   # local dev
            contexts, active_context = config.list_kube_config_contexts()
            self.cluster_name = active_context['context']['cluster']
        except:
            config.load_incluster_config()  # inside cluster
            cfg = client.Configuration.get_default_copy()
            self.cluster_name = cfg.host.replace("https://", "").split(":")[0]

        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()

    def fetch_resources(self):
        resources = []
        relations = []

        # --- Cluster Node ---
        cluster_id = f"cluster:{self.cluster_name}"
        resources.append({
            "id": cluster_id,
            "type": "Cluster",
            "name": self.cluster_name
        })

        # --- Namespaces ---
        for ns in self.v1.list_namespace().items:
            ns_id = f"namespace:{ns.metadata.name}"
            resources.append({
                "id": ns_id,
                "type": "Namespace",
                "name": ns.metadata.name
            })

        # --- Nodes ---
        for node in self.v1.list_node().items:
            node_id = f"node:{node.metadata.name}"
            resources.append({
                "id": node_id,
                "type": "Node",
                "name": node.metadata.name
            })
            relations.append({
                "src": cluster_id,
                "dst": node_id,
                "type": "HAS"
            })

        # --- Pods and Containers ---
        for pod in self.v1.list_pod_for_all_namespaces().items:
            pod_id = f"pod:{pod.metadata.namespace}:{pod.metadata.name}"
            resources.append({
                "id": pod_id,
                "type": "Pod",
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "node": pod.spec.node_name
            })

            # Pod -> Namespace
            relations.append({
                "src": f"namespace:{pod.metadata.namespace}",
                "dst": pod_id,
                "type": "CONTAINS"
            })

            # Pod -> Node
            if pod.spec.node_name:
                relations.append({
                    "src": f"node:{pod.spec.node_name}",
                    "dst": pod_id,
                    "type": "SCHEDULED_ON"
                })

            # Containers inside Pod
            for container in pod.spec.containers:
                container_id = f"container:{pod.metadata.namespace}:{pod.metadata.name}:{container.name}"
                resources.append({
                    "id": container_id,
                    "type": "Container",
                    "name": container.name,
                    "image": container.image,
                    "pod": pod.metadata.name
                })

                relations.append({
                    "src": pod_id,
                    "dst": container_id,
                    "type": "HAS_CONTAINER"
                })

        # --- Services ---
        for svc in self.v1.list_service_for_all_namespaces().items:
            svc_id = f"service:{svc.metadata.namespace}:{svc.metadata.name}"
            resources.append({
                "id": svc_id,
                "type": "Service",
                "name": svc.metadata.name,
                "namespace": svc.metadata.namespace,
                "serviceType": svc.spec.type
            })

            # Service -> Namespace
            relations.append({
                "src": f"namespace:{svc.metadata.namespace}",
                "dst": svc_id,
                "type": "CONTAINS"
            })

            # Relation service -> pods via selector
            selector = svc.spec.selector or {}
            if selector:
                # Construire label selector Kubernetes sous forme de dict
                for pod in self.v1.list_namespaced_pod(svc.metadata.namespace,
                                               label_selector=",".join([f"{k}={v}" for k, v in selector.items()])).items:
                    pod_id = f"pod:{pod.metadata.namespace}:{pod.metadata.name}"
                    relations.append({
                        "src": svc_id,
                        "dst": pod_id,
                        "type": "SELECTS"  # ou SERVES selon ton vocabulaire
                    })

        # --- Deployments ---
        for dep in self.apps_v1.list_deployment_for_all_namespaces().items:
            dep_id = f"deployment:{dep.metadata.namespace}:{dep.metadata.name}"
            resources.append({
                "id": dep_id,
                "type": "Deployment",
                "name": dep.metadata.name,
                "namespace": dep.metadata.namespace,
                "replicas": dep.spec.replicas
            })

            # Deployment -> Namespace
            relations.append({
                "src": f"namespace:{dep.metadata.namespace}",
                "dst": dep_id,
                "type": "CONTAINS"
            })

            # Deployment -> Pods (via label selector)
            selector = dep.spec.selector.match_labels or {}
            if selector:
                selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
                pods = self.v1.list_namespaced_pod(dep.metadata.namespace, label_selector=selector_str).items
                for pod in pods:
                    pod_id = f"pod:{pod.metadata.namespace}:{pod.metadata.name}"
                    relations.append({
                        "src": dep_id,
                        "dst": pod_id,
                        "type": "CONTROLS"
                    })

        # --- ReplicaSets ---
        for rs in self.apps_v1.list_replica_set_for_all_namespaces().items:
            rs_id = f"replicaset:{rs.metadata.namespace}:{rs.metadata.name}"
            resources.append({
                "id": rs_id,
                "type": "ReplicaSet",
                "name": rs.metadata.name,
                "namespace": rs.metadata.namespace,
                "replicas": rs.spec.replicas
            })

            # ReplicaSet -> Deployment (ownerReference)
            for owner in rs.metadata.owner_references or []:
                if owner.kind == "Deployment":
                    dep_id = f"deployment:{rs.metadata.namespace}:{owner.name}"
                    relations.append({
                        "src": dep_id,
                        "dst": rs_id,
                        "type": "MANAGES"
                    })

            # ReplicaSet -> Pods
            selector = rs.spec.selector.match_labels or {}
            if selector:
                selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
                pods = self.v1.list_namespaced_pod(rs.metadata.namespace, label_selector=selector_str).items
                for pod in pods:
                    pod_id = f"pod:{pod.metadata.namespace}:{pod.metadata.name}"
                    relations.append({
                        "src": rs_id,
                        "dst": pod_id,
                        "type": "CONTROLS"
                    })

        return resources, relations
