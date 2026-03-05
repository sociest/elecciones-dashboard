import requests, json
import pandas as pd
import time
import os

query = """SELECT ?item ?label ?ci ?cargo ?militancia ?trayectoria ?estudios ?foto ?youtube ?facebook ?instagram ?tiktok ?twitter ?partido ?territorio
WHERE { 
  ?item claim:69857da6142c6cf1636b ?stmt1 .
  OPTIONAL {
    ?item claim:69839e7ca5dfc05c1847 ?stmt2 .
    ?stmt2 value: ?ci .
    ?item claim:69909ff678ca509e132b ?stmt3 .
    ?stmt3 value: ?militancia .
    ?item claim:6991e1f71d9b946eed02 ?stmt4 .
    ?stmt4 value: ?trayectoria .
    ?item claim:698a960962e87e866083 ?stmt5 .
    ?stmt5 value: ?estudios .
    ?item claim:698d2b149e3a7aa9ca9d ?stmt6 .
    ?stmt6 value: ?foto .
    ?item claim:698ff396819084d3f34f ?stmt7 .
    ?stmt7 value: ?youtube .
    ?item claim:698a9704d5423dd2a594 ?stmt8 .
    ?stmt8 value: ?facebook .
    ?item claim:6990ac7d411c99d182eb ?stmt9 .
    ?stmt9 value: ?instagram .
    ?item claim:698d2ea93ec1314cd130 ?stmt10 .
    ?stmt10 value: ?tiktok .
    ?item claim:6990acbb7e77c6674b88 ?stmt11 .
    ?stmt11 value: ?twitter .
    ?item claim:69857da6142c6cf1636b ?stmt12 .
    ?stmt12 value: ?cargo .
    ?stmt12 qual:6985697dce1378ac55e9 ?partido .
    ?stmt12 qual:6982cd215f22d1c5d613 ?territorio .
  }
}
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://appwrite.sociest.org/v1/functions/69a736320015c4f40b23/executions"

# Cargar variables de entorno o usar valores por defecto
project_id = os.getenv("APPWRITE_PROJECT", "697ea96f003c3264105c")
api_key = os.getenv("APPWRITE_API_KEY", "")

# Fetch all candidates with pagination
offset = 0
limit = 100
max_iterations = 80

all_data = []

# Headers que simulan un navegador real para evitar Cloudflare
headers = {
    "Content-Type": "application/json",
    "X-Appwrite-Project": project_id,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://appwrite.sociest.org",
    "Referer": "https://appwrite.sociest.org/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1"
}

# Agregar API key si está disponible
if api_key:
    headers["X-Appwrite-Key"] = api_key

print("Start Extraction")
print(f"Project ID: {project_id[:20]}...")
print(f"API Key: {'Configurada' if api_key else 'No configurada'}")

for i in range(max_iterations):
    query_with_offset = query.replace("{{OFFSET}}", str(offset)).replace("{{LIMIT}}", str(limit))
    
    payload = {
        "body": json.dumps({
            "query": query_with_offset.replace('\n', '\n')
        }),
        "method": "POST",
        "headers": {"Content-Type": "application/json"}
    }
    
    max_retries = 3
    response = None
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting request {attempt + 1}/3 for offset {offset}...")
            
            # Crear sesión para mantener cookies
            session = requests.Session()
            session.headers.update(headers)
            
            response = session.post(endpoint, json=payload, timeout=900, allow_redirects=False)
            
            if response.status_code == 201:
                print(f"Success!")
                break
            elif response.status_code == 403:
                print(f"403 Forbidden - Posiblemente Cloudflare. Headers: {list(response.headers.keys())[:5]}")
                if attempt < max_retries - 1:
                    print(f"Esperando 5 segundos antes de reintentar...")
                    time.sleep(5)
            else:
                print(f"Error {response.status_code}: {response.text[:200]}")
                if attempt < max_retries - 1:
                    print(f"Reintentando en 5 segundos...")
                    time.sleep(5)
        except Exception as e:
            print(f"Exception: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Reintentando en 5 segundos...")
                time.sleep(5)
    
    if response is None or response.status_code != 201:
        print(f"Request failed after {max_retries} attempts")
        if response:
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text[:500]}")
        break
    
    response_data = response.json()
    response_body = json.loads(response_data["responseBody"])
    results = response_body.get("results", {}).get("bindings", [])
    
    if not results:
        print(f"No more records at offset {offset}")
        break
    
    print(f"Offset {offset}: Retrieved {len(results)} records")
    all_data.extend(results)
    offset += limit
    time.sleep(1)

print(f"\nTotal candidates collected: {len(all_data)}")

if all_data:
    all_data_df = pd.DataFrame(all_data)
    all_data_df.to_csv("datos/candidatos.csv", index=False)
    print("Data saved to datos/candidatos.csv")
else:
    print("No data collected. Check authentication and permissions.")

