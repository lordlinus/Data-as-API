import fastapi

description = """
Demo FastAPI with Cosmos DB backend from Syntetic(fake) data

## Customer

You can **read items** by passing Customer id e.g. LA-SG_20746123.

## Policy

You can **read items** by passing Policy id e.g. LA-SG_166923771. 

## Options

You can **read items** by passing Options id e.g. LA-SG_943340287.

## Agent

You can **read items** by passing Options id e.g. LA-SG_21086094.

## API

A sample api response that connects all the above apis e.g. LA-SG_20746123
"""
tags_metadata = [
    {
        "name": "customers",
        "description": "e.g. LA-SG_20746123",
    },
    {
        "name": "policy",
        "description": "e.g. LA-SG_166923771",
    },
    {
        "name": "agent",
        "description": "e.g. LA-SG_21086094",
    },
    {
        "name": "options",
        "description": "e.g. LA-SG_943340287",
    },
    {
        "name": "api",
        "description": "Sample API try with LA-SG_166923771",
    },
]

app = fastapi.FastAPI(
    title="Sample API for testing",
    description=description,
    version="0.0.1",
    # terms_of_service="http://example.com/terms/",
    # contact={
    #     "name": "You know me",
    #     "url": "http://example.com/contact/",
    #     "email": "hello@example.com",
    # },
    # license_info={
    #     "name": "None",
    #     "url": "http://NA",
    # },
)

# TODO: Add setup or configuration specifications for the application here
