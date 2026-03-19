import os

from collect_common import collect_query_to_csv

query = """SELECT DISTINCT 
  ?item ?label ?foto
  ?cargo ?cargo_label
  ?ci 
  ?militancia ?militancia_label ?militancia_logo
  ?trayectoria ?trayectoria_label ?trayectoria_logo
  ?estudios ?estudios_label ?estudios_logo
  ?partido ?partido_label ?partido_logo
  ?territorio ?territorio_label ?territorio_codigo
  ?youtube ?facebook ?instagram ?tiktok ?twitter
WHERE { 
  # --- ANCLA: Buscar por cargo ---
  ?item claim:69857da6142c6cf1636b ?cargo_stmt .
  ?cargo_stmt value: ?cargo .
  
  # --- ETIQUETAS Y ATRIBUTOS DIRECTOS ---
  OPTIONAL { ?item label: ?label . }
  OPTIONAL { ?item claim:698d2b149e3a7aa9ca9d ?foto_stmt . ?foto_stmt value: ?foto . }
  OPTIONAL { ?item claim:69839e7ca5dfc05c1847 ?ci_stmt . ?ci_stmt value: ?ci . }
  
  # --- ETIQUETA DEL CARGO (como entidad) ---
  OPTIONAL { ?cargo label: ?cargo_label . }
  
  # --- MILITANCIA ---
  OPTIONAL {
    ?item claim:69909ff678ca509e132b ?mil_stmt .
    ?mil_stmt value: ?militancia .
    ?militancia label: ?militancia_label .
    ?militancia claim:6985e79eafa03f6c8186 ?mil_logo_stmt .
    ?mil_logo_stmt value: ?militancia_logo .
  }
  
  # --- TRAYECTORIA ---
  OPTIONAL {
    ?item claim:6991e1f71d9b946eed02 ?tray_stmt .
    ?tray_stmt value: ?trayectoria .
    ?trayectoria label: ?trayectoria_label .
    ?trayectoria claim:6985e79eafa03f6c8186 ?trayectoria_logo_stmt .
    ?trayectoria_logo_stmt value: ?trayectoria_logo .
  }
  
  # --- ESTUDIOS ---
  OPTIONAL {
    ?item claim:698a960962e87e866083 ?est_stmt .
    ?est_stmt value: ?estudios .
    ?estudios label: ?estudios_label .
    ?estudios claim:6985e79eafa03f6c8186 ?estudios_logo_stmt .
    ?estudios_logo_stmt value: ?estudios_logo .
  }
  
  # --- CUALIFICADORES DEL CARGO ---
  OPTIONAL {
    ?cargo_stmt qual:6985697dce1378ac55e9 ?partido .
    OPTIONAL { 
      ?partido label: ?partido_label . 
      ?partido claim:6985e79eafa03f6c8186 ?partido_logo_stmt .
      ?partido_logo_stmt value: ?partido_logo .
    }
  }
  OPTIONAL {
    ?cargo_stmt qual:6982cd215f22d1c5d613 ?territorio .
    OPTIONAL { 
      ?territorio label: ?territorio_label .
      ?territorio claim:6982c462b97710531954 ?cod_stmt .
      ?cod_stmt value: ?territorio_codigo .
    }
  }
  
  # --- REDES SOCIALES (acceso directo) ---
  OPTIONAL { ?item claim:698ff396819084d3f34f ?yt_stmt . ?yt_stmt value: ?youtube . }
  OPTIONAL { ?item claim:698a9704d5423dd2a594 ?fb_stmt . ?fb_stmt value: ?facebook . }
  OPTIONAL { ?item claim:6990ac7d411c99d182eb ?ig_stmt . ?ig_stmt value: ?instagram . }
  OPTIONAL { ?item claim:698d2ea93ec1314cd130 ?tk_stmt . ?tk_stmt value: ?tiktok . }
  OPTIONAL { ?item claim:6990acbb7e77c6674b88 ?tw_stmt . ?tw_stmt value: ?twitter . }
}
ORDER BY ?item
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://query.sociest.org/"

max_records_env = os.getenv("MAX_RECORDS", 9000)
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