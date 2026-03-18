import os

from collect_common import collect_query_to_csv

query = """SELECT DISTINCT ?item ?label ?logo ?sigla ?sede ?presidente ?militantes ?fundador ?fundacion ?ideologia ?descripcion ?colores ?tipo
WHERE {
  OPTIONAL { ?item claim:6985e79eafa03f6c8186 ?ret_stmt3 . ?ret_stmt3 value: ?logo . }
  OPTIONAL { ?item claim:69912af042192aa886da ?ret_stmt4 . ?ret_stmt4 value: ?sigla . }
  OPTIONAL { ?item claim:699631a380986c7458fa ?ret_stmt5 . ?ret_stmt5 value: ?sede . }
  OPTIONAL { ?item claim:6991dc50c8c1754deb3b ?ret_stmt6 . ?ret_stmt6 value: ?presidente . }
  OPTIONAL { ?item claim:69935ad42b7b4f5f6022 ?ret_stmt7 . ?ret_stmt7 value: ?militantes . }
  OPTIONAL { ?item claim:6991dc2eabb96fde2475 ?ret_stmt8 . ?ret_stmt8 value: ?fundador . }
  OPTIONAL { ?item claim:69932d145f0576125f96 ?ret_stmt9 . ?ret_stmt9 value: ?fundacion . }
  OPTIONAL { ?item claim:699359f4c98d1096a2d6 ?ret_stmt10 . ?ret_stmt10 value: ?ideologia . }
  OPTIONAL { ?item claim:69932bedd9f3e515b846 ?ret_stmt11 . ?ret_stmt11 value: ?descripcion . }
  OPTIONAL { ?item claim:69860e969885b01c0bb4 ?ret_stmt12 . ?ret_stmt12 value: ?colores . }
  OPTIONAL { ?item claim:69935d76d5af6efd12ea ?ret_stmt13 . ?ret_stmt13 value: ?tipo . }
  ?item claim:69814ee90009513e4f69 ?stmt0 .
  ?stmt0 value: "6985697dce1378ac55e9" .
}
ORDER BY ?item
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://query.sociest.org/"

max_records_env = os.getenv("MAX_RECORDS")
max_records = int(max_records_env) if max_records_env else None

collect_query_to_csv(
    query_template=query,
    format="csv",
    output_path="datos/partidos.csv",
    endpoint=endpoint,
    initial_limit=int(os.getenv("PAGE_LIMIT", "100")),
    max_retries=int(os.getenv("MAX_RETRIES", "3")),
    request_timeout=int(os.getenv("REQUEST_TIMEOUT", "900")),
    retry_delay=int(os.getenv("RETRY_DELAY", "5")),
    sleep_between_pages=float(os.getenv("SLEEP_BETWEEN_PAGES", "1")),
    max_records=max_records,
)