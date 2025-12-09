from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
import json

# CONNECT TO MONGODB
client = MongoClient("mongodb://localhost:27017/")
db = client["company_scrape"]
input_collection = db["input_companies"]              # your existing collection
output_collection = db["parsed_companies"]     # new parsed results



# CLEAN TEXT FUNCTION
def clean(text):
    if not text:
        return ""
    return " ".join(text.strip().split())

# PARSE TECH STACK
def parse_tech_stack(soup):
    items = []
    tech_section = soup.select("#tech li")
    for li in tech_section:
        name = clean(li.select_one(".name").get_text() if li.select_one(".name") else "")
        category = clean(li.select_one(".category").get_text() if li.select_one(".category") else "")
        if name:
            items.append({"name": name, "category": category})
    return items

# PARSE EMAIL FORMATS
def parse_email_formats(soup):
    formats = []
    rows = soup.select("#email table tbody tr")
    for r in rows:
        cols = r.find_all("td")
        if len(cols) >= 3:
            formats.append({
                "pattern": clean(cols[0].get_text()),
                "example": clean(cols[1].get_text()),
                "percentage": clean(cols[2].get_text())
            })
    return formats

# PARSE FAQ SECTION
def parse_faq(soup):
    faqs = []
    faq_block = soup.select("#faq details")
    for d in faq_block:
        question = clean(d.select_one("summary h3").get_text()) if d.select_one("summary h3") else ""
        answer = clean(d.get_text().replace(question, ""))
        if question:
            faqs.append({"question": question, "answer": answer})
    return faqs

# PARSE COMPANY OVERVIEW SECTION
def parse_overview(soup):
    result = {}
    rows = soup.select(".highlight-left dl .item")
    for row in rows:
        key_tag = row.select_one("dt")
        value_tag = row.select_one("dd")
        if key_tag and value_tag:
            key = clean(key_tag.get_text())
            val = clean(value_tag.get_text())
            result[key] = val
    return result

# PARSE LEADERSHIP
def parse_leadership(soup):
    leaders = []
    section = soup.select(".sidebar-top ul li")

    for li in section:
        name = li.select_one(".name strong")
        title = li.select_one(".title")

        if name:
            leaders.append({
                "name": clean(name.get_text()),
                "title": clean(title.get_text() if title else "")
            })

    return leaders

# PARSE SOCIAL LINKS
def parse_social_links(soup):
    links = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "linkedin.com" in href:
            links["linkedin"] = href
    return links

# GET MAIN DESCRIPTION
def parse_description(soup):
    pre = soup.select_one(".hero pre")
    return clean(pre.get_text()) if pre else ""

# MAIN PARSER
def parse_html(html):
    soup = BeautifulSoup(html, "lxml")

    data = {}

    # Basic meta tags
    data["title"] = clean(soup.title.text if soup.title else "")
    meta_desc = soup.find("meta", attrs={"name": "description"})
    data["meta_description"] = clean(meta_desc["content"]) if meta_desc else ""

    # Extract major fields
    data["overview"] = parse_overview(soup)
    data["faq"] = parse_faq(soup)
    data["email_formats"] = parse_email_formats(soup)
    data["tech_stack"] = parse_tech_stack(soup)
    data["leadership"] = parse_leadership(soup)
    data["social_links"] = parse_social_links(soup)
    data["description"] = parse_description(soup)

    return data

# PROCESS ALL DOCUMENTS FROM MONGODB
def process_all():
    docs = input_collection.find()

    for doc in docs:
        company_id = doc.get("company_id")
        company_name = doc.get("company_name")
        html = doc.get("scraped_data", {}).get("html_content", "")

        if not html:
            print("Skipping missing html for:", doc["_id"])
            continue

        parsed = parse_html(html)

        final_doc = {
            "company_id": company_id,
            "company_name": company_name,
            "main_url": doc.get("main_url"),
            "related_urls": doc.get("related_urls", []),
            "scraped_at": doc.get("scraped_at"),
            "parsed_data": parsed,
        }

        output_collection.insert_one(final_doc)
        print("Inserted:", company_name)


if __name__ == "__main__":
    process_all()
