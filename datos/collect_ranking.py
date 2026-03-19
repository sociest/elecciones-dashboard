import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from appwrite.client import Client
from appwrite.services.tables_db import TablesDB
from appwrite.services.users import Users

try:
	from appwrite.query import Query
except Exception:
	Query = None

from dotenv import load_dotenv

load_dotenv()


def _normalize_endpoint(endpoint: str) -> str:
	endpoint = endpoint.rstrip("/")
	if endpoint.endswith("/v1"):
		return endpoint
	return f"{endpoint}/v1"


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
	if isinstance(obj, dict):
		return obj.get(key, default)
	value = getattr(obj, key, None)
	if value is not None:
		return value

	data = getattr(obj, "data", None)
	if isinstance(data, dict):
		return data.get(key, default)

	return default


def _safe_get_any(obj: Any, keys: List[str], default: Any = None) -> Any:
	for key in keys:
		value = _safe_get(obj, key, None)
		if value is not None:
			return value
	return default


def _q_limit(limit: int) -> str:
	if Query:
		return Query.limit(limit)
	return f"limit({limit})"


def _q_order_asc(field: str) -> str:
	if Query:
		return Query.order_asc(field)
	return f"orderAsc({field})"


def _q_cursor_after(row_id: str) -> str:
	if Query:
		return Query.cursor_after(row_id)
	return f"cursorAfter({row_id})"


def _get_rows(response: Any) -> List[Dict[str, Any]]:
	rows = _safe_get(response, "rows")
	if rows is None:
		rows = _safe_get(response, "documents", [])
	return rows or []


def _clean(value: Any) -> Optional[str]:
	if value is None:
		return None
	text = str(value).strip()
	if not text:
		return None
	lowered = text.lower()
	if lowered in {"null", "none", "nan"}:
		return None
	return text


def _min_iso(a: Optional[str], b: Optional[str]) -> Optional[str]:
	if not a:
		return b
	if not b:
		return a
	return a if a <= b else b


def _max_iso(a: Optional[str], b: Optional[str]) -> Optional[str]:
	if not a:
		return b
	if not b:
		return a
	return a if a >= b else b


def fetch_all_audit_rows(
	tables_db: TablesDB,
	database_id: str,
	table_id: str,
	page_limit: int,
	max_rows: int,
) -> List[Dict[str, Any]]:
	all_rows: List[Dict[str, Any]] = []
	cursor_after: Optional[str] = None

	while True:
		remaining = max_rows - len(all_rows)
		if remaining <= 0:
			print(f"Se alcanzó el límite máximo de {max_rows} filas.")
			break

		current_limit = min(page_limit, remaining)
		queries = [_q_limit(current_limit), _q_order_asc("$id")]
		if cursor_after:
			queries.append(_q_cursor_after(cursor_after))

		response = tables_db.list_rows(
			database_id=database_id,
			table_id=table_id,
			queries=queries,
		)

		rows = _get_rows(response)
		if not rows:
			break

		all_rows.extend(rows)
		new_cursor = _safe_get_any(rows[-1], ["$id", "id"])
		if not new_cursor:
			print("No se pudo obtener cursor de paginación. Se detiene.")
			break

		if new_cursor == cursor_after:
			print("Cursor sin avance detectado. Se detiene para evitar loop infinito.")
			break

		cursor_after = new_cursor

		print(f"Filas acumuladas: {len(all_rows)}")

		if len(rows) < current_limit:
			break

	return all_rows


def aggregate_contributors(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	contributors: Dict[str, Dict[str, Any]] = defaultdict(dict)

	for row in rows:
		user_id = _clean(_safe_get_any(row, ["user_id", "userId", "userid"]))
		user_name = _clean(_safe_get_any(row, ["user_name", "userName", "username"]))

		if not user_id and not user_name:
			continue

		normalized_name = (user_name or "").lower()
		key = user_id or f"name::{normalized_name}"

		created_at = _clean(_safe_get_any(row, ["$createdAt", "createdat", "createdAt"]))
		action = _clean(_safe_get(row, "action"))
		entity_type = _clean(_safe_get(row, "entity_type"))

		if not contributors[key]:
			contributors[key] = {
				"user_id": user_id,
				"user_name": user_name,
				"total_cambios": 0,
				"primera_actividad": None,
				"ultima_actividad": None,
				"acciones": set(),
				"entity_types": set(),
			}

		if user_name and not contributors[key]["user_name"]:
			contributors[key]["user_name"] = user_name
		if user_id and not contributors[key]["user_id"]:
			contributors[key]["user_id"] = user_id

		contributors[key]["total_cambios"] += 1
		contributors[key]["primera_actividad"] = _min_iso(
			contributors[key]["primera_actividad"], created_at
		)
		contributors[key]["ultima_actividad"] = _max_iso(
			contributors[key]["ultima_actividad"], created_at
		)

		if action:
			contributors[key]["acciones"].add(action)
		if entity_type:
			contributors[key]["entity_types"].add(entity_type)

	output: List[Dict[str, Any]] = []
	for c in contributors.values():
		output.append(
			{
				"user_id": c["user_id"] or "",
				"user_name": c["user_name"] or "",
				"total_cambios": c["total_cambios"],
				"primera_actividad": c["primera_actividad"] or "",
				"ultima_actividad": c["ultima_actividad"] or "",
				"acciones": "; ".join(sorted(c["acciones"])),
				"entity_types": "; ".join(sorted(c["entity_types"])),
			}
		)

	output.sort(
		key=lambda x: (
			-int(x["total_cambios"]),
			x["ultima_actividad"] or "",
			x["user_name"] or "",
		),
		reverse=False,
	)
	return output


def _resolve_user_name_from_users_service(
	users_service: Users,
	user_id: str,
	cache: Dict[str, str],
) -> str:
	if user_id in cache:
		return cache[user_id]

	resolved_name = ""
	try:
		user = users_service.get(user_id=user_id)
		resolved_name = _clean(_safe_get_any(user, ["name", "email"])) or ""
	except Exception as exc:
		print(f"No se pudo resolver user_name para user_id={user_id}: {exc}")

	cache[user_id] = resolved_name
	return resolved_name


def enrich_missing_user_names(
	contributors: List[Dict[str, Any]],
	users_service: Optional[Users],
) -> List[Dict[str, Any]]:
	if not users_service:
		return contributors

	cache: Dict[str, str] = {}
	enriched = 0

	for contributor in contributors:
		user_id = _clean(contributor.get("user_id"))
		user_name = _clean(contributor.get("user_name"))

		if user_name or not user_id:
			continue

		resolved = _resolve_user_name_from_users_service(
			users_service=users_service,
			user_id=user_id,
			cache=cache,
		)
		if resolved:
			contributor["user_name"] = resolved
			enriched += 1

	if enriched:
		print(f"user_name completado desde Users API: {enriched}")

	contributors.sort(
		key=lambda x: (
			-int(x["total_cambios"]),
			x["ultima_actividad"] or "",
			x["user_name"] or "",
		),
		reverse=False,
	)

	return contributors


def main() -> None:
	endpoint = os.getenv("APPWRITE_ENDPOINT") or os.getenv("APPWRITE_URL")
	project_id = os.getenv("APPWRITE_PROJECT_ID")
	api_key = os.getenv("APPWRITE_API_KEY")
	database_id = os.getenv("APPWRITE_DATABASE_ID")
	table_id = os.getenv("APPWRITE_AUDIT_TABLE_ID", "audit_log")
	output_path = os.getenv("RANKING_OUTPUT_PATH", "datos/ranking.csv")
	page_limit = int(os.getenv("PAGE_LIMIT", "100"))
	max_rows = int(os.getenv("MAX_AUDIT_ROWS", "80000"))

	missing = []
	if not endpoint:
		missing.append("APPWRITE_ENDPOINT (o APPWRITE_URL)")
	if not project_id:
		missing.append("APPWRITE_PROJECT_ID")
	if not api_key:
		missing.append("APPWRITE_API_KEY")
	if not database_id:
		missing.append("APPWRITE_DATABASE_ID")

	if missing:
		raise RuntimeError(
			"Faltan variables de entorno requeridas: " + ", ".join(missing)
		)

	endpoint = cast(str, endpoint)
	project_id = cast(str, project_id)
	api_key = cast(str, api_key)
	database_id = cast(str, database_id)

	client = Client()
	client.set_endpoint(_normalize_endpoint(endpoint))
	client.set_project(project_id)
	client.set_key(api_key)

	tables_db = TablesDB(client)
	users_service = Users(client)

	print(f"Leyendo tabla '{table_id}' de Appwrite...")
	rows = fetch_all_audit_rows(
		tables_db=tables_db,
		database_id=database_id,
		table_id=table_id,
		page_limit=page_limit,
		max_rows=max_rows,
	)
	print(f"Filas de auditoría recuperadas: {len(rows)}")

	ranking = aggregate_contributors(rows)
	print(f"Contribuidores detectados: {len(ranking)}")
	ranking = enrich_missing_user_names(ranking, users_service)

	pd.DataFrame(ranking).to_csv(output_path, index=False)
	print(f"Ranking guardado en: {output_path}")


if __name__ == "__main__":
	main()
