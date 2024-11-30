from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from ScoreCalculation import TxnGraphScore, accountAge
import os
import requests
from pymongo import MongoClient
import sqlite3
from typing import Dict
import asyncio
import threading

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://htf-hackermen.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment variable checks
mongo_uri = os.getenv('MONGO_URI')
etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')

if not mongo_uri:
    raise ValueError("MONGO_URI environment variable is not set.")
if not etherscan_api_key:
    raise ValueError("ETHERSCAN_API_KEY environment variable is not set.")

# MongoDB setup with pymongo
client = MongoClient(mongo_uri)
db = client["blacklistDB"]
blacklist_collection = db["blacklists"]
kyc_collection = db["kycs"]

# Request model
class EthereumRequest(BaseModel):
    address: str

# Function to check if the Ethereum address is blacklisted
def Scammer(eth_address: str) -> int:
    try:
        existing_entry = blacklist_collection.find_one({"address": eth_address})
        if existing_entry:
            return 1
        return 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking blacklist: {str(e)}")

# Function to get transactions for the Ethereum address
def getTransactions(eth_address: str) -> list:
    try:
        url = f'https://api.etherscan.io/api?module=account&action=txlist&address={eth_address}&apikey={etherscan_api_key}'
        response = requests.get(url)

        if response.status_code == 200:
            response_data = response.json()
            if 'result' in response_data:
                return response_data['result']
            else:
                raise HTTPException(status_code=400, detail="Missing 'result' field in response from Etherscan.")
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Etherscan API error: {response.text}")

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error making request to Etherscan: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Function to verify if the Ethereum address is associated with KYC
def KYCverified(eth_address: str) -> int:
    try:
        transactions = getTransactions(eth_address)

        if not transactions:
            return 0

        for tx in transactions:
            from_address = tx['from']
            to_address = tx['to']

            from_in_kyc = kyc_collection.find_one({"address": from_address})
            to_in_kyc = kyc_collection.find_one({"address": to_address})

            if from_in_kyc or to_in_kyc:
                return 1
        return 0

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking KYC: {str(e)}")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import threading

app = FastAPI()

# Define the request model
class EthereumRequest(BaseModel):
    address: str

# Function to check if the address exists in the SQLite DB
def get_address_status(address: str):
    conn = sqlite3.connect('addresses.db')
    cursor = conn.cursor()
    cursor.execute('SELECT score FROM address_scores WHERE address = ?', (address,))
    row = cursor.fetchone()
    conn.close()
    return row  # Return the row (score) if it exists, None otherwise

# Function to store the score in the SQLite DB
def store_score(address: str, score: float):
    conn = sqlite3.connect('addresses.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO address_scores (address, score) VALUES (?, ?)', (address, score))
    conn.commit()
    conn.close()

# Function to simulate asynchronous score calculation
def calculate_score(eth_address: str):
    # Simulate score calculation logic
    score = 0
    graph_score = 0
    kyc_score = 0
    age_txn_score = 0 

    # Call external functions for KYC, TxnGraph, Account Age (for example)
    if Scammer(eth_address) == 1:
        score = 1  # Direct return for scammer
        store_score(eth_address, score)
        return

    kyc_score += KYCverified(eth_address)
    graph_score += TxnGraphScore.txnGraphScore(eth_address)
    age_txn_score += accountAge.age_txn_score(eth_address)

    # Normalize scores
    graph_score *= 100
    kyc_score = 1 - kyc_score
    kyc_score *= 100
    age_txn_score = 1 - age_txn_score
    age_txn_score *= 100

    # Calculate final score
    final_score = (graph_score + kyc_score + age_txn_score) / 3

    # Store the score in the database
    store_score(eth_address, final_score)

@app.post("/process_eth_address")
async def process_eth_address(data: EthereumRequest):
    try:
        eth_address = data.address

        # Check if the score is already calculated and stored
        stored_score = get_address_status(eth_address)
        if stored_score:
            return {'score': stored_score[0], 'calculated': True}  # Return the stored score and calculated: True

        # If the score is not found in the database, return immediately with calculated: false
        # Start the score calculation in the background (asynchronously or in a separate thread)
        threading.Thread(target=calculate_score, args=(eth_address,)).start()

        return {'score': None, 'calculated': False}  # Return None as score and calculated: False

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error in processing: {str(e)}")