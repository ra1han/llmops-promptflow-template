"""
This module registers the data store.
"""
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import OneLakeDatastore, \
    OneLakeArtifact
import os
import argparse
import json

pipeline_components = []

def get_aml_client(
        subscription_id,
        resource_group_name,
        workspace_name,
):
    aml_client = MLClient(
        DefaultAzureCredential(),
        subscription_id=subscription_id,
        resource_group_name=resource_group_name,
        workspace_name=workspace_name,
    )

    return aml_client

def register_data_store(
        name_datastore,
        description,
        onelake_workspace_name,
        onelake_endpoint,
        onelake_artifact_name,
        aml_client
):
    store = OneLakeDatastore(
        name=name_datastore,
        description=description,
        one_lake_workspace_name=onelake_workspace_name,
        endpoint=onelake_endpoint,
        artifact=OneLakeArtifact(
        name=onelake_artifact_name,
        type="lake_house"
    )
    )

    aml_client.create_or_update(store)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subscription_id",
        type=str,
        help="Azure subscription id",
        required=True,
    )
    parser.add_argument(
        "--resource_group_name",
        type=str,
        help="Azure resource group",
        required=True,
    )
    parser.add_argument(
        "--workspace_name",
        type=str,
        help="Azure ML workspace",
        required=True,
    )
    parser.add_argument(
        "--config_path_root_dir",
        type=str,
        help="Root dir for config file",
        required=True,
    )

    args = parser.parse_args()

    subscription_id = args.subscription_id
    resource_group_name = args.resource_group_name
    workspace_name = args.workspace_name
    config_path_root_dir = args.config_path_root_dir

    config_path = os.path.join(os.getcwd(), f"{config_path_root_dir}/configs/dataops_config.json")
    config = json.load(open(config_path))

    onelake_config = config['ONELAKE']
    onelake_workspace_name = onelake_config['WORKSPACE_NAME']
    onelake_endpoint = onelake_config['ENDPOINT']
    onelake_artifact_name = onelake_config['ARTIFACT_NAME']

    aml_client = get_aml_client(
        subscription_id,
        resource_group_name,
        workspace_name,
    )

    register_data_store(
        name_datastore=config["DATA_STORE_NAME"],
        description=config["DATA_STORE_DESCRIPTION"],
        onelake_workspace_name=onelake_workspace_name,
        onelake_endpoint=onelake_endpoint,
        onelake_artifact_name = onelake_artifact_name,
        aml_client=aml_client
    )

if __name__ == "__main__":
    main()
