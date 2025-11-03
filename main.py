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
    """
    Lista arquivos de uma pasta ou realiza busca no Drive.
    Agora a busca é fuzzy (tolerante a variações).
    """
    try:
        service = get_service()
        q = ["trashed = false"]

        if pasta_id:
            q.append(f"'{pasta_id}' in parents")

        if query:
            # Busca por fragmentos de nome, ignorando maiúsculas/minúsculas
            termos = query.split()
            filtros = [f"name contains '{t}'" for t in termos]
            q.append("(" + " or ".join(filtros) + ")")

        query_final = " and ".join(q)

        results = service.files().list(
            q=query_final,
            fields="files(id, name, mimeType, modifiedTime)",
            pageSize=30
        ).execute()

        arquivos = results.get("files", [])
        return {"total": len(arquivos), "arquivos": arquivos}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/{file_id}")
def ler_arquivo(file_id: str):
    """
    Lê o conteúdo de um arquivo texto e retorna texto pronto para o GPT usar.
    """
    try:
        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        nome = file.get("name")
        mime = file.get("mimeType", "")

        # Faz o download do conteúdo
        from io import BytesIO
        from googleapiclient.http import MediaIoBaseDownload
        import docx2txt

        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)

        if mime == "application/vnd.google-apps.document":
            texto = "Google Docs não pode ser baixado diretamente — converta para DOCX primeiro."
        elif mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            with open("temp.docx", "wb") as temp:
                temp.write(fh.read())
            texto = docx2txt.process("temp.docx")
        elif "text/" in mime:
            texto = fh.read().decode("utf-8", errors="ignore")
        else:
            texto = f"Formato não suportado para leitura automática: {mime}"

        return {
            "nome": nome,
            "tipo": mime,
            "conteudo": texto[:15000]  # Limite para evitar sobrecarga
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def root():
    return {"message": "✅ AIDA Drive Connector está ativo!"}

