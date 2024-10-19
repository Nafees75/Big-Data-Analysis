from pymongo import MongoClient

def extract_medalists():
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['Olympic_Athletes']
    collection = db['Olympic_Athletes']

    # Query to fetch all Summer Olympics medal-winning athletes
    query = {
        'edition': {'$regex': 'Summer Olympics'},  # Match Summer Olympics
        'medal': {'$in': ['Gold', 'Silver', 'Bronze']}  # Only medal winners
    }

    # Open the file to write results
    with open('athletes.txt', 'w', encoding='utf-8') as file:
        for athlete in collection.find(query):
            # Extract year from the 'edition' field
            edition = athlete['edition']
            year = edition.split()[0]  # Extract only the year from the edition field
            
            # Only include athletes from 1980 to 2020
            if 1980 <= int(year) <= 2020:
                # Format and write the data with semicolons as delimiters
                file.write(f"{athlete['athlete_id']}; {athlete['country_noc']}; {year}; {athlete['event']}; {athlete['medal']}\n")

    print("Data extraction complete.")

# Run the function
extract_medalists()
