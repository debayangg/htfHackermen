from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from ScoreCalculation import TxnGraphScore, accountAge
import os
import requests
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

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

# Asynchronous route to process Ethereum address
@app.post("/process_eth_address")
async def process_eth_address(data: EthereumRequest):
    try:
        eth_address = data.address
        score = 0
        graph_score = 0
        kyc_score = 0
        age_txn_score = 0 

        # Scammer and KYC functions are synchronous, so they can be called directly
        if Scammer(eth_address) == 1:
            return {'score':1}
        
        kyc_score += KYCverified(eth_address)
        print('Passed KYC check')
        graph_score += TxnGraphScore.txnGraphScore(eth_address)
        print('Passed Txn Graph check')
        age_txn_score += accountAge.age_txn_score(eth_address)
        print('Passed account age check')

        graph_score *= 100
        kyc_score = 1 - kyc_score
        kyc_score *= 100
        
        
        age_txn_score = 1 - age_txn_score
        age_txn_score *= 100

        # Calculate the final score
        final_score = (graph_score + kyc_score + age_txn_score) / 3
        
        return {'score': 100 - final_score}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error in processing: {str(e)}")