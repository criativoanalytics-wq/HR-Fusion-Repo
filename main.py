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
# 🚀 AIDA DRIVE CONNECTOR - RAG VERSION
# ============================================================
# Este backend permite ao ChatGPT buscar, abrir e ler arquivos
# do Google Drive (.docx, .pdf, .txt) automaticamente, sem
# precisar pedir autorização ao usuário.
# ============================================================

app = FastAPI(
    title="AIDA Drive Connector",
    description="API RAG para leitura e busca automática no Google Drive",
    version="2.0.0"
)

# Configuração de CORS (permite acesso do ChatGPT e de apps externos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Escopo mínimo necessário para leitura do Drive
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
# 📁 Listagem de arquivos
# ============================================================
@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    """Lista arquivos de uma pasta ou faz busca textual no Drive."""
    try:
        service = get_service()
        q = []
        if pasta_id:
            q.append(f"'{pasta_id}' in parents")
        if query:
            q.append(f"name contains '{query}'")
        q.append("trashed=false")
        query_final = " and ".join(q)

        results = service.files().list(
            q=query_final,
            fields="files(id, name, mimeType, modifiedTime)",
            pageSize=50
        ).execute()
        return {"arquivos": results.get("files", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos: {e}")

# ============================================================
# 📄 Leitura e extração de conteúdo
# ============================================================
@app.get("/files/{file_id}")
def ler_arquivo(file_id: str):
    """
    Faz download e extrai texto automaticamente de arquivos do Google Drive.
    Suporta: DOCX, PDF, TXT. Retorna o texto limpo para o GPT processar.
    """
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

        # --------------------------------------------------------
        # 🧩 DOCX
        # --------------------------------------------------------
        if mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as temp_file:
                temp_file.write(fh.read())
                temp_path = temp_file.name
            texto_extraido = docx2txt.process(temp_path)
            os.remove(temp_path)

        # --------------------------------------------------------
        # 📘 PDF
        # --------------------------------------------------------
        elif mime == "application/pdf":
            reader = PdfReader(fh)
            texto_extraido = "\n".join([p.extract_text() or "" for p in reader.pages])

        # --------------------------------------------------------
        # 📄 TXT
        # --------------------------------------------------------
        elif "text" in mime:
            texto_extraido = fh.read().decode("utf-8", errors="ignore")

        # --------------------------------------------------------
        # ❗ Outros formatos
        # --------------------------------------------------------
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
    return {"message": "✅ AIDA Drive Connector RAG está ativo e pronto para uso."}
