from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
import os
import tempfile
import docx2txt
from PyPDF2 import PdfReader

# ============================================================
# 🚀 AIDA DRIVE CONNECTOR - RAG VERSION (Multilíngue e Smart)
# ============================================================

app = FastAPI(
    title="AIDA Drive Connector",
    description="API RAG multilíngue para leitura e busca semântica no Google Drive (.docx, .pdf, .txt)",
    version="2.1.0"
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
# 🔐 Autenticação
# ============================================================
def get_service():
    """Cria o serviço autenticado do Google Drive."""
    if not os.path.exists("token.json"):
        raise HTTPException(status_code=401, detail="Token OAuth ausente. Gere o token primeiro com auth_setup.py")
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    return build("drive", "v3", credentials=creds)

# ============================================================
# 🧠 Dicionário de sinônimos bilíngue
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
    """Expande automaticamente termos equivalentes em PT/EN e gera busca case-insensitive."""
    if not query:
        return []

    query_lower = query.lower().strip()
    termos_expandidos = {query_lower}

    for chave, sinonimos in SINONIMOS.items():
        if chave in query_lower or any(s in query_lower for s in sinonimos):
            termos_expandidos.add(chave)
            termos_expandidos.update(sinonimos)

    # Garante unicidade
    return list(set(termos_expandidos))

# ============================================================
# 📁 Listagem de arquivos (com expansão bilíngue)
# ============================================================
@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    """
    Lista arquivos de uma pasta ou faz busca textual no Drive.
    - Expande automaticamente a busca com sinônimos bilíngues.
    - Ignora maiúsculas/minúsculas.
    """
    try:
        service = get_service()
        termos_busca = expandir_termos(query)

        if not termos_busca:
            termos_busca = [query.lower()] if query else []

        arquivos_encontrados = []
        ids_vistos = set()

        for termo in termos_busca or [""]:
            q = []
            if pasta_id:
                q.append(f"'{pasta_id}' in parents")
            if termo:
                q.append(f"name contains '{termo}'")
            q.append("trashed=false")
            query_final = " and ".join(q)

            results = service.files().list(
                q=query_final,
                fields="files(id, name, mimeType, modifiedTime)",
                pageSize=100
            ).execute()

            for f in results.get("files", []):
                if f["id"] not in ids_vistos:
                    arquivos_encontrados.append(f)
                    ids_vistos.add(f["id"])

        return {"arquivos": arquivos_encontrados}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos: {e}")

# ============================================================
# 📄 Leitura e extração de conteúdo
# ============================================================
@app.get("/files/{file_id}")
def ler_arquivo(file_id: str):
    """Faz download e extrai texto automaticamente de arquivos do Google Drive (.docx, .pdf, .txt)."""
    try:
        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        nome = file["name"]
        mime = file["mimeType"]

        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        texto_extraido = ""

        # DOCX
        if mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as temp_file:
                temp_file.write(fh.read())
                temp_path = temp_file.name
            texto_extraido = docx2txt.process(temp_path)
            os.remove(temp_path)

        # PDF
        elif mime == "application/pdf":
            reader = PdfReader(fh)
            texto_extraido = "\n".join([p.extract_text() or "" for p in reader.pages])

        # TXT
        elif "text" in mime:
            texto_extraido = fh.read().decode("utf-8", errors="ignore")

        else:
            texto_extraido = f"O tipo de arquivo {mime} não é suportado para leitura direta."

        if not texto_extraido.strip():
            texto_extraido = "⚠️ O arquivo foi encontrado, mas parece não conter texto legível."

        return {
            "nome": nome,
            "tipo": mime,
            "conteudo": texto_extraido[:50000]  # Limite de segurança
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler arquivo: {e}")

# ============================================================
# 🔍 Endpoint raiz
# ============================================================
@app.get("/")
def root():
    return {"message": "✅ AIDA Drive Connector RAG (multilíngue) está ativo e pronto para uso."}

if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.getenv("PORT", 8080))  # 👈 Render injeta a variável PORT
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
