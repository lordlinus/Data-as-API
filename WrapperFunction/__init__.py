import logging
import os

import nest_asyncio

nest_asyncio.apply()
import azure.functions as func

from DataAPI import build_api_response, get_id
from FastAPIApp import app  # Main API application

endpoint = os.getenv("testcosmos01_ENDPOINT")
key = os.getenv("testcosmos01_KEY")
database_name = os.getenv("testcosmos01_DB")


@app.get("/customer/{id}")
async def customerInfo(id):
    return await get_id("customer", id)


@app.get("/policy/{id}")
async def policyInfo(id):
    return await get_id("policy", id)


@app.get("/agent/{id}")
async def agentInfo(id):
    return await get_id("agent", id)


@app.get("/options/{id}")
async def optionsInfo(id):
    return await get_id("options", id)


@app.get("/api/{customerid}")
async def sampleAPI(customerid):
    return await build_api_response(customerid)


async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return func.AsgiMiddleware(app).handle(req, context)
