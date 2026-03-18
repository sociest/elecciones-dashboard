import os

from collect_common import collect_query_to_csv

query = """SELECT DISTINCT ?item ?label ?ci ?cargo ?cargo_label ?militancia ?militancia_label ?trayectoria ?trayectoria_label ?estudios ?estudios_label ?partido ?partido_label ?territorio ?territorio_label ?foto ?youtube ?facebook ?instagram ?tiktok ?twitter
WHERE { 
  # Campo obligatorio por el que se ancla la búsqueda
  ?item claim:69857da6142c6cf1636b ?stmt1 .
  ?stmt1 value: ?cargo .
  
  # Etiquetas explícitas para campos principales
  OPTIONAL { ?item label: ?label . }
  OPTIONAL { ?cargo label: ?cargo_label . }

  # Bloques opcionales independientes con su propia etiqueta
  OPTIONAL { 
    ?item prop:69839e7ca5dfc05c1847 ?ci . 
  }
  OPTIONAL { 
    ?item prop:69909ff678ca509e132b ?militancia . 
    ?militancia label: ?militancia_label .
  }
  OPTIONAL { 
    ?item prop:6991e1f71d9b946eed02 ?trayectoria . 
    ?trayectoria label: ?trayectoria_label .
  }
  OPTIONAL { 
    ?item prop:698a960962e87e866083 ?estudios . 
    ?estudios label: ?estudios_label .
  }
  
  # Redes Sociales y Multimedia
  OPTIONAL { ?item prop:698d2b149e3a7aa9ca9d ?foto . }
  OPTIONAL { ?item prop:698ff396819084d3f34f ?youtube . }
  OPTIONAL { ?item prop:698a9704d5423dd2a594 ?facebook . }
  OPTIONAL { ?item prop:6990ac7d411c99d182eb ?instagram . }
  OPTIONAL { ?item prop:698d2ea93ec1314cd130 ?tiktok . }
  OPTIONAL { ?item prop:6990acbb7e77c6674b88 ?twitter . }
  
  # Cualificadores del Cargo con etiquetas explícitas
  OPTIONAL { 
    ?stmt1 qual:6985697dce1378ac55e9 ?partido . 
    ?partido label: ?partido_label .
  }
  OPTIONAL { 
    ?stmt1 qual:6982cd215f22d1c5d613 ?territorio . 
    ?territorio label: ?territorio_label .
  }
}
ORDER BY ?item
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://query.sociest.org/"

max_records_env = os.getenv("MAX_RECORDS", 7000)
max_records = int(max_records_env) if max_records_env else None

collect_query_to_csv(
    query_template=query,
    format="csv",
    output_path="datos/candidatos.csv",
    endpoint=endpoint,
    initial_limit=int(os.getenv("PAGE_LIMIT", "100")),
    max_retries=int(os.getenv("MAX_RETRIES", "3")),
    request_timeout=int(os.getenv("REQUEST_TIMEOUT", "900")),
    retry_delay=int(os.getenv("RETRY_DELAY", "5")),
    sleep_between_pages=float(os.getenv("SLEEP_BETWEEN_PAGES", "1")),
    max_records=max_records,
)