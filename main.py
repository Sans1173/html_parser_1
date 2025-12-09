from bs4 import BeautifulSoup
from pymongo import MongoClient
from multiprocessing import Pool
import time
import logging

# Global

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "company_scrape"
INPUT_COLLECTION_NAME = "input_companies"
OUTPUT_COLLECTION_NAME = "scraped_companies"
READ_BATCH_SIZE = 2000
INSERT_BATCH_SIZE = 500
NUM_WORKERS = 8
LOG_LEVEL = "INFO"

# CONNECT TO MONGODB (YOUR REQUIRED BLOCK)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
input_collection = db[INPUT_COLLECTION_NAME]
output_collection = db[OUTPUT_COLLECTION_NAME]


# LOGGING
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s: %(message)s"
)

# WORKER PARSER FUNCTION
def parse_worker(doc):
    """Runs inside parallel processes."""
    try:
        html = doc.get("html", "")
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        # Basic meta data
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        meta = soup.find("meta", attrs={"name": "description"})
        meta_description = meta["content"].strip() if meta and meta.get("content") else ""

        # Pre-extract heavy DOM sections once
        tech_div = soup.find(id="tech")
        email_div = soup.find(id="email")
        faq_div = soup.find(id="faq")
        overview_items = soup.select(".highlight-left dl .item")
        leadership_items = soup.select(".sidebar-top ul li")
        description_pre = soup.select_one(".hero pre")

        # Overview
        overview = {}
        for row in overview_items or []:
            dt = row.find("dt")
            dd = row.find("dd")
            if dt and dd:
                overview[dt.get_text(" ", strip=True)] = dd.get_text(" ", strip=True)

        # Tech Stack
        tech_stack = []
        if tech_div:
            for li in tech_div.find_all("li"):
                name_el = li.find(class_="name")
                cat_el = li.find(class_="category")
                name = name_el.get_text(" ", strip=True) if name_el else ""
                category = cat_el.get_text(" ", strip=True) if cat_el else ""
                if name:
                    tech_stack.append({"name": name, "category": category})

        # Email Formats
        email_formats = []
        if email_div:
            tbody = email_div.find("tbody")
            rows = tbody.find_all("tr") if tbody else email_div.find_all("tr")
            for r in rows:
                cols = r.find_all("td")
                if len(cols) >= 3:
                    email_formats.append({
                        "pattern": cols[0].get_text(" ", strip=True),
                        "example": cols[1].get_text(" ", strip=True),
                        "percentage": cols[2].get_text(" ", strip=True)
                    })

        # FAQs
        faqs = []
        if faq_div:
            details = faq_div.find_all("details")
            for d in details:
                question_el = d.find("summary")
                question = question_el.get_text(" ", strip=True) if question_el else ""
                full = d.get_text(" ", strip=True)
                answer = full.replace(question, "").strip()
                if question:
                    faqs.append({"question": question, "answer": answer})

        # Leadership
        leadership = []
        for li in leadership_items or []:
            name_el = li.find(class_="name")
            title_el = li.find(class_="title")

            name = ""
            if name_el:
                strong = name_el.find("strong")
                name = strong.get_text(" ", strip=True) if strong else name_el.get_text(" ", strip=True)

            title = title_el.get_text(" ", strip=True) if title_el else ""

            if name:
                leadership.append({"name": name, "title": title})

        # Social Links
        social_links = {}
        for a in soup.find_all("a", href=True):
            if "linkedin.com" in a["href"]:
                social_links["linkedin"] = a["href"]

        # Description
        description = description_pre.get_text(" ", strip=True) if description_pre else ""

        # Final parsed structure
        return {
            "company_id": doc.get("company_id"),
            "company_name": doc.get("company_name"),
            "main_url": doc.get("main_url"),
            "related_urls": doc.get("related_urls", []),
            "scraped_at": doc.get("scraped_at"),
            "parsed_data": {
                "title": title,
                "meta_description": meta_description,
                "overview": overview,
                "tech_stack": tech_stack,
                "email_formats": email_formats,
                "faq": faqs,
                "leadership": leadership,
                "social_links": social_links,
                "description": description,
            }
        }

    except Exception as e:
        logging.error(f"Parse failed for doc: {e}")
        return None

# PROCESS ALL DOCUMENTS
def process_all(read_batch=READ_BATCH_SIZE, 
    insert_batch=INSERT_BATCH_SIZE, 
    workers=NUM_WORKERS):

    logging.info("Starting ETL job...")

    # Only required fields fetched (faster)
    projection = {
        "company_id": 1,
        "company_name": 1,
        "main_url": 1,
        "related_urls": 1,
        "scraped_at": 1,
        "scraped_data.html_content": 1
    }

    cursor = input_collection.find({}, projection).batch_size(500)

    total = 0
    start = time.time()

    # Parallel pool
    with Pool(processes=workers) as pool:

        batch = []
        for doc in cursor:
            batch.append({
                "company_id": doc.get("company_id"),
                "company_name": doc.get("company_name"),
                "main_url": doc.get("main_url"),
                "related_urls": doc.get("related_urls", []),
                "scraped_at": doc.get("scraped_at"),
                "html": doc.get("scraped_data", {}).get("html_content", "")
            })

            if len(batch) >= read_batch:
                # Parse documents in parallel
                results = pool.map(parse_worker, batch, chunksize=50)
                final_docs = [r for r in results if r]

                # Bulk insert
                for i in range(0, len(final_docs), insert_batch):
                    output_collection.insert_many(final_docs[i:i + insert_batch], ordered=False)

                total += len(final_docs)
                logging.info(f"Inserted {total} documents so far.")
                batch = []


        # Insert remaining
        from pymongo.errors import BulkWriteError

        if batch:
            results = pool.map(parse_worker, batch, chunksize=50)
            final_docs = [r for r in results if r]
            for i in range(0, len(final_docs), insert_batch):
                try:
                    output_collection.insert_many(final_docs[i:i + insert_batch], ordered=False)
                except BulkWriteError:
                    pass   # silently ignore duplicate key errors
            total += len(final_docs)


    logging.info(f"Completed. Total inserted: {total}")
    logging.info(f"Time taken: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    process_all()
