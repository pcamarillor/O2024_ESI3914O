import threading
import time
import random

# OOP: Define a BankAccount class to handle deposits and withdrawals
class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance
        self.lock = threading.Lock()  # Ensure thread safety

    def deposit(self, amount):
        with self.lock:
            time.sleep(random.randint(0, 10))  # Simulate delay
            self.balance += amount
            print(f"Deposited {amount}, new balance is {self.balance}")

    def withdraw(self, amount):
        with self.lock:
            time.sleep(random.randint(0, 10))  # Simulate delay
            if amount <= self.balance:
                self.balance -= amount
                print(f"Withdrew {amount}, new balance is {self.balance}")
            else:
                print("Insufficient funds")

    def get_balance(self):
        with self.lock:
            return self.balance

    def transfer(self, target_account, amount):
        with self.lock:
            if amount <= self.balance:
                self.withdraw(amount)
                target_account.deposit(amount)
                print(f"Transferred {amount} to target account. New balance: {self.balance}")
            else:
                print("Insufficient funds to transfer")

def process_transactions(account, transactions):
    threads = []
    for function, *args in transactions:
        t = threading.Thread(target=function, args=(account, *args))
        threads.append(t)
        t.start()

    _ = [t.join() for t in threads]

# Define wrapper functions
def deposit(account, amount):
    account.deposit(amount)

def withdraw(account, amount):
    account.withdraw(amount)

def transfer(account, target_account, amount):
    account.transfer(target_account, amount)

# Example usage
if __name__ == "__main__":
    account_a = BankAccount(1000)  # Create an account with an initial balance
    account_b = BankAccount(3000)

    transactions_a = [
        (deposit, 200),
        (withdraw, 150),
        (deposit, 300),
        (withdraw, 500),
        (deposit, 100)
    ]

    transactions_b = [
        (deposit, 500),
        (withdraw, 400),
        (deposit, 800),
        (withdraw, 300)
    ]

    # Separate list for transfers
    transfer_transactions = [
        (transfer, account_b, 200),  # Transfer from account_a to account_b
        (transfer, account_a, 300)   # Transfer from account_b to account_a
    ]

    # Process transactions on account_a
    process_transactions(account_a, transactions_a)

    # Process transactions on account_b
    process_transactions(account_b, transactions_b)

    # Process transfer transactions
    process_transactions(account_a, transfer_transactions)  # Transfers involving account_a
    process_transactions(account_b, transfer_transactions)  # Transfers involving account_b

    # Print the final balances
    print(f"Final Balance of Account A: {account_a.get_balance()}")
    print(f"Final Balance of Account B: {account_b.get_balance()}")
