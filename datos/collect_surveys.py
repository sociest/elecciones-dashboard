import os

from collect_common import collect_query_to_csv

query = """
SELECT DISTINCT ?item ?label ?cobertura ?cobertura_label ?autor ?autor_label ?fecha_fin ?fecha_inicio ?margen ?muestra ?resultado ?resultado_label ?resultadoporcentaje ?resultadopregunta ?publicacion ?nivel_confianza ?archivo ?casa_registrada ?ciudadanos_registrados ?censo_personas16
WHERE {
  # Filtro inicial obligatorio
  ?item claim:69814ee90009513e4f69 ?stmt0 .
  ?stmt0 value: "698ff11ee732836cda65" .

  OPTIONAL { 
    # Datos básicos del item
    ?item label: ?stmt_label . ?stmt_label value: ?label .
    
    # Cobertura
    ?item claim:698febc2001abeb954c4 ?ret_stmt3 . ?ret_stmt3 value: ?cobertura .
    ?cobertura label: ?stmt_label_cober . ?stmt_label_cober value: ?cobertura_label .
    
    # Autor
    ?item claim:698ff2fb95a70777d5e8 ?ret_stmt4 . ?ret_stmt4 value: ?autor .
    ?autor label: ?stmt_label_aut . ?stmt_label_aut value: ?autor_label .
    
    # Métricas
    ?item claim:69821fd781d266c39049 ?ret_stmt5 . ?ret_stmt5 value: ?fecha_fin .
    ?item claim:698a967bd8e7eecbcef3 ?ret_stmt6 . ?ret_stmt6 value: ?fecha_inicio .
    ?item claim:698caa881742f3dbb3b4 ?ret_stmt7 . ?ret_stmt7 value: ?margen .
    ?item claim:698caa3f0d32d780c789 ?ret_stmt8 . ?ret_stmt8 value: ?muestra .
    
    # Resultados y sus Calificadores (Importante: cual: cuelga de ?ret_stmt9)
    ?item claim:698cad71c5f2134e49f9 ?ret_stmt9 . 
    ?ret_stmt9 value: ?resultado .
    
    # Si ?resultado es un ID de entidad, ?resultado_label se llenará solo si la incluyes en el SELECT
    # Pero si quieres usar el triple explícito:
    ?resultado label: ?ret_stmt_res_lab . ?ret_stmt_res_lab value: ?resultado_label .
    
    # Calificadores del resultado
    ?ret_stmt9 qual:698cac4104ba13afd2c0 ?qual_stmt1 . ?qual_stmt1 value: ?resultadoporcentaje .
    ?ret_stmt9 qual:699de9dc7562ee112101 ?qual_stmt2 . ?qual_stmt2 value: ?resultadopregunta .
    
    # Metadatos extra
    ?item claim:699ddf64a6cc230024bb ?ret_stmt10 . ?ret_stmt10 value: ?publicacion .
    ?item claim:699893d5d05f1a329cb4 ?ret_stmt11 . ?ret_stmt11 value: ?nivel_confianza .
    ?item claim:6985e9c7598e2f77b9d3 ?ret_stmt12 . ?ret_stmt12 value: ?archivo .
    
    # Característica Booleana
    ?autor claim:69814ee90009513e4f69 ?ret_stmt13 . 
    ?ret_stmt13 value:69860f45c802b1fdedb7 ?casa_registrada .
    
    # Datos de ciudadanos y censo
    ?cobertura claim:69892ef13a40d8c7a3c7 ?ret_stmt14 . ?ret_stmt14 value: ?ciudadanos_registrados .
    ?cobertura claim:69857deeecda0a266b57 ?ret_stmt15 . ?ret_stmt15 value: ?censo_personas16 . 
  }
}
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://query.sociest.org/"

max_records_env = os.getenv("MAX_RECORDS")
max_records = int(max_records_env) if max_records_env else None

collect_query_to_csv(
    query_template=query,
    format="csv",
    output_path="datos/encuestas.csv",
    endpoint=endpoint,
    initial_limit=int(os.getenv("PAGE_LIMIT", "100")),
    max_retries=int(os.getenv("MAX_RETRIES", "3")),
    request_timeout=int(os.getenv("REQUEST_TIMEOUT", "900")),
    retry_delay=int(os.getenv("RETRY_DELAY", "5")),
    sleep_between_pages=float(os.getenv("SLEEP_BETWEEN_PAGES", "1")),
    max_records=max_records,
)
