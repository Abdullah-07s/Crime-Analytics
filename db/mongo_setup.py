from pymongo import MongoClient
import yaml
import os

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

client = MongoClient(config['mongodb']['uri'])
db = client[config['mongodb']['database']]

def setup_mongo():
    """Create MongoDB collections and indexes for the crime analytics system."""
    # Create collections
    if config['mongodb']['alert_collection'] not in db.list_collection_names():
        db.create_collection(config['mongodb']['alert_collection'])
    
    if config['mongodb']['raw_events_collection'] not in db.list_collection_names():
        db.create_collection(config['mongodb']['raw_events_collection'])

    # Create indexes
    db[config['mongodb']['alert_collection']].create_index([("district", 1)])
    db[config['mongodb']['alert_collection']].create_index([("timestamp", -1)])
    db[config['mongodb']['raw_events_collection']].create_index([("CASE NUMBER", 1)])

    print(f"MongoDB setup complete for database: {config['mongodb']['database']}")

if __name__ == "__main__":
    setup_mongo()
