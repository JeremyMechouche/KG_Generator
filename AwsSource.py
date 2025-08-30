import boto3

class AwsSource:
    def __init__(self, region_name="eu-west-3"):
        self.region = region_name
        self.ec2 = boto3.client("ec2", region_name=region_name)

    def fetch_resources(self):
        resources = []
        relations = []

        # --- AWS Account root node ---
        account_id = boto3.client("sts").get_caller_identity().get("Account")
        aws_root_id = f"aws:{account_id}"
        resources.append({
            "id": aws_root_id,
            "type": "AwsAccount",
            "name": account_id
        })

        # --- VPCs ---
        vpcs = self.ec2.describe_vpcs()["Vpcs"]
        for vpc in vpcs:
            vpc_id = f"vpc:{vpc['VpcId']}"
            resources.append({
                "id": vpc_id,
                "type": "Vpc",
                "cidr": vpc.get("CidrBlock"),
                "state": vpc.get("State")
            })
            relations.append({
                "src": aws_root_id,
                "dst": vpc_id,
                "type": "HAS"
            })

        # --- Subnets ---
        subnets = self.ec2.describe_subnets()["Subnets"]
        for subnet in subnets:
            subnet_id = f"subnet:{subnet['SubnetId']}"
            vpc_id = f"vpc:{subnet['VpcId']}"
            resources.append({
                "id": subnet_id,
                "type": "Subnet",
                "cidr": subnet.get("CidrBlock"),
                "az": subnet.get("AvailabilityZone")
            })
            relations.append({
                "src": vpc_id,
                "dst": subnet_id,
                "type": "CONTAINS"
            })

        # --- EC2 Instances ---
        instances = self.ec2.describe_instances()["Reservations"]
        for res in instances:
            for inst in res["Instances"]:
                inst_id = f"ec2:{inst['InstanceId']}"
                subnet_id = f"subnet:{inst['SubnetId']}" if inst.get("SubnetId") else None
                tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}

                resources.append({
                    "id": inst_id,
                    "type": "EC2Instance",
                    "state": inst["State"]["Name"],
                    "instanceType": inst["InstanceType"],
                    "az": inst["Placement"]["AvailabilityZone"],
                    "labels": tags
                })

                if subnet_id:
                    relations.append({
                        "src": subnet_id,
                        "dst": inst_id,
                        "type": "RUNS_IN"
                    })

        return resources, relations
