from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.oauth2.credentials import Credentials
from io import BytesIO
from datetime import datetime
import os, re, json, tempfile as tmp, numpy as np, time
from functools import lru_cache

# ============================================================
# 🚀 AIDA DRIVE CONNECTOR - Versão otimizada (Lazy + EN priority)
# ============================================================

app = FastAPI(
    title="AIDA Drive Connector",
    description="API RAG multilíngue otimizada (EN prioridade, PT sob demanda) para leitura e busca semântica no Google Drive (.docx, .pdf, .txt)",
    version="2.3.0"
)

# ============================================================
# 🌐 CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# ============================================================
# 🧠 Lazy loading spaCy (prioridade EN, fallback PT)
# ============================================================
_nlp_cache = {}

def detect_language_hint(text: str) -> str:
    """Detecta indícios de idioma para priorizar EN/PT."""
    text_lower = text.lower()
    portugues_indicios = ["dados", "governança", "integração", "qualidade", "projeto", "migração", "relatório"]
    if any(word in text_lower for word in portugues_indicios):
        return "pt"
    return "en"

def get_nlp(lang_hint: str = None):
    """Carrega o modelo spaCy sob demanda, priorizando EN."""
    import spacy
    lang = lang_hint or "en"
    model_name = "en_core_web_sm" if lang == "en" else "pt_core_news_sm"

    if lang not in _nlp_cache:
        print(f"🔤 Carregando modelo spaCy: {model_name}")
        _nlp_cache[lang] = spacy.load(model_name)

    return _nlp_cache[lang]

def detectar_pessoa_spacy(texto: str):
    """Detecta nomes de pessoas com prioridade EN e fallback PT."""
    if not texto:
        return []

    lang_hint = detect_language_hint(texto)
    pessoas = set()

    # Analisa primeiro no idioma provável
    nlp = get_nlp(lang_hint)
    for ent in nlp(texto).ents:
        if ent.label_ == "PERSON":
            pessoas.add(ent.text.strip())

    # Fallback no outro idioma, se nada encontrado
    if not pessoas:
        fallback_lang = "pt" if lang_hint == "en" else "en"
        nlp_fallback = get_nlp(fallback_lang)
        for ent in nlp_fallback(texto).ents:
            if ent.label_ == "PERSON":
                pessoas.add(ent.text.strip())

    return list(pessoas)

# ============================================================
# 🔐 Lazy loading Google Drive API
# ============================================================
@lru_cache()
def get_service():
    from googleapiclient.discovery import build
    if not os.path.exists("token.json"):
        raise HTTPException(status_code=401, detail="Token OAuth ausente. Gere o token com auth_setup.py.")
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    return build("drive", "v3", credentials=creds)

# ============================================================
# 🧩 Dicionário de sinônimos bilíngue
# ============================================================
SINONIMOS = {
    "governança de dados": ["data governance", "gestão de dados", "política de dados", "data management"],
    "qualidade de dados": ["data quality", "data cleansing", "data validation"],
    "catálogo de dados": ["data catalog", "metadata management"],
    "lago de dados": ["data lake", "data repository"],
    "segurança da informação": ["information security", "data privacy", "cybersecurity"],
    "arquitetura de dados": ["data architecture", "data modeling", "data structure"],
    "integração de dados": ["data integration", "ETL", "data ingestion"],
    "governança": ["governance", "management", "oversight"],
}

def expandir_termos(query: str):
    """Expande termos equivalentes em PT/EN."""
    if not query:
        return []
    query_lower = query.lower().strip()
    termos_expandidos = {query_lower}
    for chave, sinonimos in SINONIMOS.items():
        if chave in query_lower or any(s in query_lower for s in sinonimos):
            termos_expandidos.add(chave)
            termos_expandidos.update(sinonimos)
    return list(set(termos_expandidos))

# ============================================================
# 🧠 Lazy loading SentenceTransformer e FAISS
# ============================================================
@lru_cache()
def get_embedding_model():
    from sentence_transformers import SentenceTransformer
    print("⚙️ Carregando modelo de embeddings (MiniLM)...")
    return SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

def load_faiss_index(index_path="index_cache/transcripts.index", meta_path="index_cache/transcripts_meta.json"):
    import faiss
    if not os.path.exists(index_path) or not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail="Índice FAISS não encontrado. Execute /index_transcripts primeiro.")
    index = faiss.read_index(index_path)
    with open(meta_path, "r", encoding="utf-8") as f:
        metadados = json.load(f)
    return index, metadados

# ============================================================
# 📁 Listagem de arquivos
# ============================================================
@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    try:
        service = get_service()
        termos_busca = expandir_termos(query)
        arquivos_encontrados, ids_vistos = [], set()

        for termo in termos_busca or [""]:
            q = []
            if pasta_id:
                q.append(f"'{pasta_id}' in parents")
            if termo:
                q.append(f"name contains '{termo}'")
            q.append("trashed=false")
            query_final = " and ".join(q)

            page_token = None
            while True:
                results = service.files().list(
                    q=query_final,
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, parents)",
                    pageSize=100,
                    pageToken=page_token
                ).execute()

                for f in results.get("files", []):
                    if f["id"] not in ids_vistos:
                        arquivos_encontrados.append(f)
                        ids_vistos.add(f["id"])

                page_token = results.get("nextPageToken")
                if not page_token:
                    break

        return {"arquivos": arquivos_encontrados, "total": len(arquivos_encontrados)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos: {e}")

# ============================================================
# 🔍 Busca semântica nos transcripts (FAISS + ST lazy)
# ============================================================
@app.get("/search_transcripts")
def search_transcripts(query: str, top_k: int = 5):
    try:
        model = get_embedding_model()
        index, metadados = load_faiss_index()

        query_embedding = model.encode([query])
        distances, indices = index.search(np.array(query_embedding, dtype=np.float32), top_k)

        resultados = []
        for idx, dist in zip(indices[0], distances[0]):
            if idx < len(metadados):
                item = metadados[idx]
                resultados.append({
                    "arquivo": item["arquivo"],
                    "file_id": item["file_id"],
                    "trecho": item["trecho"],
                    "similaridade": float(1 - dist / 2)
                })

        return {"query": query, "total_resultados": len(resultados), "resultados": resultados}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar no índice FAISS: {e}")

# ============================================================
# 🧾 Indexação de transcripts (otimizada)
# ============================================================
@app.get("/index_transcripts")
def indexar_transcripts(pasta_raiz: str = None):
    try:
        inicio = time.time()
        service = get_service()
        model = get_embedding_model()

        termos_reuniao = ["transcript", "meeting", "reunião", "minutes", "call", "discussion", "notes"]
        q_filter = " or ".join([f"name contains '{t}'" for t in termos_reuniao])
        q = f"({q_filter}) and trashed=false"
        if pasta_raiz:
            q += f" and '{pasta_raiz}' in parents"

        results = service.files().list(q=q, fields="files(id,name,mimeType,modifiedTime)", pageSize=500).execute()
        arquivos = results.get("files", [])
        if not arquivos:
            return {"status": "⚠️ Nenhum transcript encontrado."}

        os.makedirs("index_cache", exist_ok=True)
        embeddings_list, metadados = [], []

        for f in arquivos:
            try:
                from googleapiclient.http import MediaIoBaseDownload
                import docx
                file_id, nome = f["id"], f["name"]
                request = service.files().get_media(fileId=file_id)
                fh = BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                fh.seek(0)

                with tmp.NamedTemporaryFile(delete=False, suffix=".docx") as temp_file:
                    temp_file.write(fh.read())
                    temp_path = temp_file.name

                doc = docx.Document(temp_path)
                os.remove(temp_path)
                paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
                if not paragraphs:
                    continue

                buffer = ""
                for paragraph in paragraphs:
                    if len(buffer) + len(paragraph) < 5000:
                        buffer += paragraph + "\n"
                    else:
                        embeddings_list.append(model.encode(buffer))
                        metadados.append({"arquivo": nome, "file_id": file_id, "trecho": buffer[:300] + "..."})
                        buffer = paragraph + "\n"

                if buffer:
                    embeddings_list.append(model.encode(buffer))
                    metadados.append({"arquivo": nome, "file_id": file_id, "trecho": buffer[:300] + "..."})

            except Exception as e:
                print(f"⚠️ Erro em {f['name']}: {e}")

        if not embeddings_list:
            return {"status": "Nenhum trecho válido encontrado."}

        import faiss
        embeddings = np.array(embeddings_list, dtype=np.float32)
        index = faiss.IndexFlatL2(embeddings.shape[1])
        index.add(embeddings)

        faiss.write_index(index, "index_cache/transcripts.index")
        with open("index_cache/transcripts_meta.json", "w", encoding="utf-8") as f:
            json.dump(metadados, f, ensure_ascii=False, indent=2)

        return {
            "status": "✅ Indexação concluída.",
            "total_arquivos": len(arquivos),
            "total_chunks": len(embeddings_list),
            "tempo_execucao_seg": round(time.time() - inicio, 2)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao indexar transcripts: {e}")

# ============================================================
# 🩵 Endpoint raiz
# ============================================================
@app.get("/")
def root():
    return {"message": "✅ AIDA Drive Connector ativo (EN prioridade, lazy loading pronto para Render)"}

# ============================================================
# 🚀 Execução Render-friendly
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    print(f"🌐 Servidor iniciado na porta {port} (modo leve).")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
