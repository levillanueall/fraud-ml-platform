from fastapi import FastAPI
from pydantic import BaseModel, Field
import pickle

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

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/transaction")
def create_transaction(transaction: Transaction):

    risk_score = calculate_risk(transaction)

    decision = "fraud" if risk_score >= 0.5 else "normal"

    return {
        "message": "Transaction processed",
        "risk_score": risk_score,
        "decision": decision,
        "transaction": transaction
    }