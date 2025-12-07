# html_parser_1
This project processes thousands of company HTML pages stored in MongoDB, extracts meaningful structured information (overview, tech stack, FAQs, leadership, email formats, etc.), and stores the parsed data into a new collection.

Optimized using multiprocessing, batch processing, and bulk inserts, this system can efficiently handle 10,000 to 100,000+ HTML documents.

Features: 
Reads raw HTML from MongoDB (input_companies)
Stores parsed JSON into MongoDB (scraped_companies)
Uses multiprocessing for high-speed parsing
Batch reading and batch insertion for performance
Error handling + logging included

Instruction:
Make sure your MongoDB is running locally or update the connection string in the script:

client = MongoClient("mongodb://localhost")
db = client["company_scrape"]
input_collection = db["input_companies"]
output_collection = db["scraped_companies"]

Output:
Script Will Automatically:
Read documents in batches
Process them using multiple CPU cores
Insert cleaned JSON into scraped_companies

Tech Stack:
Python 3.8+
BeautifulSoup4 for HTML parsing
pymongo for MongoDB interactions
lxml parser
multiprocessing for parallel execution
