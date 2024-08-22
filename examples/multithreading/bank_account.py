import threading
import time
import random
from functools import reduce

# OOP: Define a BankAccount class to handle deposits and withdrawals
class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance
        self.lock = threading.Lock()  # Ensure thread safety

    def deposit(self, amount):
        with self.lock:
            time.sleep(random.randint(0, 3))
            self.balance += amount
            print(f"Deposited {amount}, new balance is {self.balance}")

    def withdraw(self, amount):
        with self.lock:
            time.sleep(random.randint(0, 3))
            if amount <= self.balance:
                self.balance -= amount
                print(f"Withdrew {amount}, new balance is {self.balance}")
            else:
                print("Insufficient funds")

    def transfer(self, target_account, amount):
        with self.lock:
            time.sleep(random.randint(0, 3))
            if self.balance >= amount:
                self.balance -= amount
                target_account.balance += amount
                print(f"Transferred {amount}")
            else:
                print(f"Transfer failed. Insufficient funds.")

    def get_balance(self):
        with self.lock:
            return self.balance

def process_transactions(transactions):
    threads = []
    for function, amount in transactions:
        t = threading.Thread(target=function, args=(account, amount))
        threads.append(t)
        t.start()

    _ = [t.join() for t in threads]

# Define wrapper functions
def deposit(account, amount):
    account.deposit(amount)

def withdraw(account, amount):
    account.withdraw(amount)
# Example usage
if __name__ == "__main__":
    account = BankAccount(1000)  # Create an account with an initial balance
    account_b = BankAccount(3000)
    transactions = [
        (deposit, 200),
        (withdraw, 150),
        (deposit, 300),
        (withdraw, 500),
        (deposit, 100)
    ]

    # Process transactions using multiple threads
    process_transactions(transactions)


    transfers = [
        (account_b, account, 109),
        (account, account_b, 200) 
    ]

    transfer_threads = []
    transfer_threads.append(threading.Thread(target=account_b.transfer, args=(account, 117)))
    transfer_threads.append(threading.Thread(target=account_b.transfer, args=(account, 13)))
    transfer_threads.append(threading.Thread(target=account.transfer, args=(account_b, 23)))
    transfer_threads.append(threading.Thread(target=account.transfer, args=(account_b, 51)))

    _ = [t.start() for t in transfer_threads]

    _ = [t.join() for t in transfer_threads]


    # Print the final balance
    print(f"Final Balance: {account.get_balance()}")
