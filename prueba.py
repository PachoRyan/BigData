import requests
import gzip
import json
import pandas as pd
from datetime import datetime
import locale

# Establecer idioma en español
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

# =============================
# 1. Descargar y cargar el archivo
# =============================
url = "https://data.gharchive.org/2025-01-02-15.json.gz"
file_path = "2025-01-02-15.json.gz"

print("Descargando archivo...")
r = requests.get(url, stream=True)
r.raise_for_status()
with open(file_path, "wb") as f:
    for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)

print("Extrayendo datos...")
with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    data = [json.loads(line) for line in f]

df = pd.DataFrame(data)
print(f"Total de filas originales: {len(df)}")

# =============================
# 2. Filtrar columnas relevantes
# =============================
cols = ['type', 'actor', 'repo', 'payload', 'created_at']
df = df[cols]

# =============================
# 3. Extraer campos útiles
# =============================
def safe_eval(x):
    if isinstance(x, dict):
        return x
    try:
        return eval(x)
    except Exception:
        return {}

df['actor_login'] = df['actor'].apply(lambda x: safe_eval(x).get('login'))
df['actor_id'] = df['actor'].apply(lambda x: safe_eval(x).get('id'))
df['repo_name'] = df['repo'].apply(lambda x: safe_eval(x).get('name'))
df['repo_id'] = df['repo'].apply(lambda x: safe_eval(x).get('id'))

def contar_commits(payload):
    payload_dict = safe_eval(payload)
    if isinstance(payload_dict, dict) and 'commits' in payload_dict:
        return len(payload_dict['commits'])
    return 0

# Nuevas columnas: tamaño del push, acción del issue o PR, branch si existe
def extraer_detalles(payload, tipo):
    payload_dict = safe_eval(payload)
    detalles = {}
    if tipo == "PushEvent":
        detalles["branch"] = payload_dict.get("ref", "").split("/")[-1]
        detalles["tamano_push"] = payload_dict.get("size", 0)
    elif tipo in ["PullRequestEvent", "IssuesEvent"]:
        detalles["action"] = payload_dict.get("action")
    return detalles

detalles = df.apply(lambda x: extraer_detalles(x['payload'], x['type']), axis=1)
df['branch'] = detalles.apply(lambda x: x.get("branch"))
df['tamano_push'] = detalles.apply(lambda x: x.get("tamano_push"))
df['action'] = detalles.apply(lambda x: x.get("action"))

df['n_commits'] = df.apply(lambda x: contar_commits(x['payload']) if x['type'] == 'PushEvent' else 0, axis=1)

# =============================
# 4. Seleccionar columnas finales
# =============================
df_filtrado = df[['created_at', 'type', 'actor_login', 'actor_id', 'repo_name', 'repo_id',
                  'branch', 'tamano_push', 'action', 'n_commits']]

# =============================
# 5. Crear documento con estructura anidada
# =============================
fecha_base = df_filtrado['created_at'].iloc[0][:10]
fecha_obj = datetime.strptime(fecha_base, "%Y-%m-%d")
dia_nombre = fecha_obj.strftime("%A").capitalize()

registers = df_filtrado.to_dict("records")

doc = {
    "date": fecha_base,
    "dia": dia_nombre,
    "id": 1,
    "registers": registers
}

# Crear varios documentos de ejemplo
docs = []
for i in range(2):  # solo 2 documentos de muestra
    d = {
        "date": doc["date"],
        "dia": doc["dia"],
        "id": i + 1,
        "registers": [doc["registers"][i]]
    }
    docs.append(d)

# Imprimir documentos formateados
for d in docs:
    print(json.dumps(d, indent=4, ensure_ascii=False))
    print("=" * 80)

'''
# =============================
# 5. Subir a MongoDB
# =============================

client = MongoClient("mongodb://localhost:27017/")  # o Atlas
db = client["gharchive_db"]
collection = db["eventos_por_dia"]

collection.insert_one(doc)

print(f"Documento insertado en MongoDB con {len(registers)} registros")
'''