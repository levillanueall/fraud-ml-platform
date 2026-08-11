from sklearn.linear_model import LogisticRegression
import pickle

X = [
    [100, 14],
    [200, 10],
    [500, 18],
    [1500, 12],
    [7000, 3],
    [9000, 2],
    [12000, 1],
    [6000, 4]
]

y = [
    0,
    0,
    0,
    0,
    1,
    1,
    1,
    1
]

model = LogisticRegression()
model.fit(X, y)

print("Coefficients:", model.coef_)
print("Intercept:", model.intercept_)

with open("model/fraud_model.pkl", "wb") as f:
    pickle.dump(model, f)