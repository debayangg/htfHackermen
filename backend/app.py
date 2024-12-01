from types import NoneType
from model.anamoly import process
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from ScoreCalculation import TxnGraphScore, accountAge
import os
import requests
from pymongo import MongoClient
import sqlite3
import queue
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager,asynccontextmanager
import threading
import asyncio

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
    allow_origins=['*'],
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
MAX_WORKERS = 10
eth_address_queue = queue.Queue()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
sqlite_lock = threading.Lock()
thread_already_running = []

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

@contextmanager
def get_db_connection():
    """SQLite connection with thread-safe settings."""
    conn = sqlite3.connect('addresses.db', check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()

def get_address_status(address: str):
    """Fetch score for an address from the SQLite database."""
    with sqlite_lock, get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT score FROM address_scores WHERE address = ?', (address,))
        return cursor.fetchone()

def store_score(address: str, score: float):
    """Update the score for an Ethereum address in the database."""
    with sqlite_lock, get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO address_scores (address, score) VALUES (?, ?)', (address, score))
        conn.commit()

async def calculate_score(eth_address: str):
    """Calculate and store the score for an Ethereum address."""
    if Scammer(eth_address) == 1:
        store_score(eth_address, 1)
        return

    kyc_score = KYCverified(eth_address)
    print(f'KYC score calculated for {eth_address}')
    graph_score = TxnGraphScore.txnGraphScore(eth_address)
    print(f'Graph score calculated for {eth_address}')
    age_txn_score = accountAge.age_txn_score(eth_address)
    print(f'Age txn score calculated for {eth_address}')
    val_store = await process(eth_address)
    print(f'ML score calculated for {eth_address}')

    # Normalize and calculate final score
    graph_score *= 100
    kyc_score = (1 - kyc_score) * 100
    age_txn_score = (1 - age_txn_score) * 100
    ml_score = val_store['prediction'][0]
    ml_score = 1 - ml_score
    ml_score *= 100
    final_score = (graph_score + kyc_score + age_txn_score + ml_score) / 4

    store_score(eth_address, final_score)

worker_count = 0  # Initialize worker count
worker_count_lock = threading.Lock()  # Lock to ensure thread-safe updates to worker_count

async def process_address(eth_address: str):
    """
    Worker function to process an Ethereum address asynchronously.
    """
    global worker_count

    print('Processing address...')
    try:
        # Calculate the score for the address
        await calculate_score(eth_address)
        print(f"Address {eth_address} processed.")
    except Exception as e:
        print(f"Error processing address {eth_address}: {e}")
    finally:
        # Decrement the worker count
        with worker_count_lock:
            thread_already_running.remove(eth_address)
            worker_count -= 1
            print(f"Worker finished for address {eth_address}. Total workers: {worker_count}")

async def worker_manager():
    """
    Worker Manager to dynamically create tasks for processing.
    Spawns new async tasks when there are items in the queue.
    """
    global worker_count

    while True:
        if not eth_address_queue.empty():
            with worker_count_lock:
                if worker_count < MAX_WORKERS:
                    eth_address = eth_address_queue.get()

                    # Add a new async task to process the address
                    asyncio.create_task(process_address(eth_address))
                    worker_count += 1
                    print(f"Worker started for address {eth_address}. Total workers: {worker_count}")

        await asyncio.sleep(0.1)  # Prevent excessive CPU usage

@app.on_event("startup")
async def startup_event():
    """
    Start the Worker Manager on application startup.
    """
    asyncio.create_task(worker_manager())
    print("Worker Manager started.")

@app.post("/process_eth_address")
async def process_eth_address(data: EthereumRequest):
    try:
        """Handle incoming requests for Ethereum address processing."""
        eth_address = data.address

        # Check if the score is already calculated and stored
        stored_score = get_address_status(eth_address)
        if type(stored_score)!=NoneType and len(stored_score)>0:
            return {'score': stored_score[0], 'calculated': True}
        # Add the address to the queue if not already being processed
        elif eth_address not in thread_already_running:
            thread_already_running.append(eth_address)
            eth_address_queue.put(eth_address)

        return {'score': None, 'calculated': False}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error in processing: {str(e)}")

@app.get("/workers_count")
def workers_count():
    return {'workers': worker_count}