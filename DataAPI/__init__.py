import asyncio
import json
import logging
import os
import time

import azure.functions as func
from azure.cosmos import PartitionKey, exceptions
from azure.cosmos.aio import CosmosClient as cosmos_client

# <read env setting for Cosmos DB>
endpoint = os.getenv("testcosmos01_ENDPOINT")
key = os.getenv("testcosmos01_KEY")
database_name = os.getenv("testcosmos01_DB")

# <create_database_if_not_exists>
async def get_or_create_db(client, database_name):
    try:
        database_obj = client.get_database_client(database_name)
        await database_obj.read()
        return database_obj
    except exceptions.CosmosResourceNotFoundError:
        logging.info("Creating database")
        return await client.create_database(database_name)


# </create_database_if_not_exists>


# <create_container_if_not_exists>
async def get_or_create_container(database_obj, container_name):
    try:
        todo_items_container = database_obj.get_container_client(container_name)
        await todo_items_container.read()
        return todo_items_container
    except exceptions.CosmosResourceNotFoundError:
        logging.error("Creating container with a partition key")
    except exceptions.CosmosHttpResponseError:
        logging.error("Fatal")
        raise


# </create_container_if_not_exists>

# <method_read_items>
async def read_items(container_obj, items_to_read, partition_key):
    # Read items (key value lookups by partition key and id, aka point reads)
    # <read_item>
    for item in items_to_read:
        item_response = await container_obj.read_item(
            item=item["id"], partition_key=item[partition_key]
        )
        request_charge = container_obj.client_connection.last_response_headers[
            "x-ms-request-charge"
        ]
        logging.info(
            f"Read item with id {item_response['id']}. Operation consumed {request_charge} request units"
        )
    # </read_item>


# <method_query_items>
async def query_items(container_obj, query_text):
    # enable_cross_partition_query should be set to True as the container is partitioned
    # In this case, we do have to await the asynchronous iterator object since logic
    # within the query_items() method makes network calls to verify the partition key
    # definition in the container
    # <query_items>
    query_items_response = container_obj.query_items(
        query=query_text, enable_cross_partition_query=True
    )
    request_charge = container_obj.client_connection.last_response_headers[
        "x-ms-request-charge"
    ]
    items = [item async for item in query_items_response]
    logging.info(
        f"Query returned {len(items)} items. Operation consumed {request_charge} request units"
    )
    return items


# </query_items>


async def build_api_response(customerid):
    logging.info(f"Build api response for {customerid}")
    # <create_cosmos_client>
    async with cosmos_client(endpoint, credential=key) as client:
        # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # create a container
            customer_obj = await get_or_create_container(database_obj, "customer")
            agent_obj = await get_or_create_container(database_obj, "agent")
            policy_obj = await get_or_create_container(database_obj, "policy")
            options_obj = await get_or_create_container(database_obj, "options")

            policy_info = await query_items(
                policy_obj,
                f"SELECT top 10 * FROM c WHERE c.customerid = '{customerid}'",
            )
            customer_info = await query_items(
                customer_obj, f"SELECT top 10 * FROM c WHERE c.id = '{customerid}'"
            )
            agent_info = await query_items(
                agent_obj,
                f"SELECT top 10 * FROM c WHERE c.agent_no = '{policy_info[0]['servingagentid']}'",
            )
            options_info = await query_items(
                options_obj,
                f"SELECT top 10 * FROM c WHERE c.policyid = '{policy_info[0]['policyno']}'",
            )
            api_response = {
                "customer": customer_info,
                "policy": policy_info,
                "agent": agent_info,
                "options": options_info,
            }
            # logging.info(api_response)
            return api_response
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"\ncaught an error. {e.message}")
        except:
            logging.error(f"\ncaught an error.")


async def get_id(container_name, id):
    logging.info(f"Build api response for {id} in {container_name}")
    # <create_cosmos_client>
    async with cosmos_client(endpoint, credential=key) as client:
        # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            container_obj = await get_or_create_container(database_obj, container_name)
            id_info = await query_items(
                container_obj, f"SELECT * FROM c WHERE c.id = '{id}'"
            )
            return {container_name: id_info}
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"\ncaught an error. {e.message}")
        except:
            logging.error(f"\ncaught an error.")


async def main(req: func.HttpRequest) -> func.HttpResponse:
    api_response = {}
    customerid = req.params.get("customerid")
    if not customerid:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("customerid")
    if customerid:
        pass

    return func.HttpResponse(
        body="Customerid is not provided or it is not found in ingested data. Try this ?customerid=LA-SG_21104577",
        status_code=404,
    )


# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
