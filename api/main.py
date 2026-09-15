from fastapi import FastAPI
from pydantic import BaseModel, Field
import pickle
from confluent_kafka import Producer
import json
import uuid

app = FastAPI()

with open("model/fraud_model.pkl", "rb") as f:
    model = pickle.load(f)

class Transaction(BaseModel): #data structure for the transaction
    customer_id: int
    amount: float = Field(gt=0)
    merchant: str
    hour: int = Field(ge=0, le=23)

def calculate_risk(transaction: Transaction) -> float:
    #risk = 0.0
    features = [[transaction.amount, transaction.hour]]
    probability = model.predict_proba(features)[0][1]  # Probability of fraud
    return probability

producer = Producer({
    "bootstrap.servers": "kafka:29092"
})

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/transaction")
def create_transaction(transaction: Transaction):

    risk_score = calculate_risk(transaction)

    decision = "fraud" if risk_score >= 0.5 else "normal"

    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": transaction.customer_id,
        "amount": transaction.amount,
        "merchant": transaction.merchant,
        "hour": transaction.hour,
        "risk_score": risk_score,
        "decision": decision
    }

    producer.produce(
        "transactions",
        value=json.dumps(event)
    )

    producer.flush()

    return {
        "message": "Transaction processed",
        "risk_score": risk_score,
        "decision": decision,
        "transaction": transaction
    }