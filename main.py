from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os
from io import BytesIO
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI(
    title="AIDA Drive Connector",
    description="API que conecta o GPT ao Google Drive para leitura e busca de arquivos.",
    version="1.0.0"
)

# Permitir acesso do ChatGPT
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def get_service():
    """Cria o serviço autenticado do Google Drive."""
    if not os.path.exists("token.json"):
        raise HTTPException(status_code=401, detail="Token OAuth ausente. Gere o token primeiro.")
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    return build("drive", "v3", credentials=creds)

@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    """Lista arquivos de uma pasta ou busca no Drive."""
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
            pageSize=20
        ).execute()
        return {"arquivos": results.get("files", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/{file_id}")
def ler_arquivo(file_id: str):
    """Lê o conteúdo de um arquivo texto ou faz download do conteúdo bruto."""
    try:
        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        mime = file.get("mimeType", "")

        # Faz o download do arquivo
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)

        if mime.startswith("text/"):
            texto = fh.read().decode("utf-8", errors="ignore")
        else:
            texto = f"Download concluído ({mime}), mas tipo de arquivo não é texto legível."

        return {
            "nome": file["name"],
            "tipo": mime,
            "conteudo": texto
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "✅ AIDA Drive Connector está ativo!"}
