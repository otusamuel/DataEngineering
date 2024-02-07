import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

def scrape_top_movies(url, limit=25):
    """Scrape top movies from a given URL."""
    movies = []
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    count = 0
    for row in rows:
        if count < limit:
            col = row.find_all('td')
            if col:
                movie = {
                    "Film": col[1].get_text(strip=True),
                    "Year": col[2].get_text(strip=True),
                    "Rotten Tomatoes' Top 100": col[3].get_text(strip=True)
                }
                movies.append(movie)
                count += 1
        else:
            break
    
    return movies

def save_to_csv(data, file_path):
    """Save data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)

def save_to_database(data, db_name, table_name):
    """Save data to a SQLite database."""
    conn = sqlite3.connect(db_name)
    df = pd.DataFrame(data)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

if __name__ == "__main__":
    url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
    db_name = 'Movies.db'
    table_name = 'Top_50'
    csv_path = '/home/project/top_25_films.csv'
    
    # Scrape top movies data
    movies_data = scrape_top_movies(url, limit=25)

    # Save to CSV
    save_to_csv(movies_data, csv_path)

    # Save to database
    save_to_database(movies_data, db_name, table_name)
