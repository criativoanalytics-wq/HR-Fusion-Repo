from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from fastapi.responses import StreamingResponse
from io import BytesIO
import os
import tempfile
import docx2txt
from PyPDF2 import PdfReader
import re
import spacy
import json
from datetime import datetime
from pptx import Presentation

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
MAX_PAYLOAD = 100_000  # Limite seguro de texto enviado
CHUNK_PAGE_SIZE = 3    # Número de páginas/slides lidos por vez

# Carrega modelos multilíngues
nlp_en = spacy.load("en_core_web_sm")
nlp_pt = spacy.load("pt_core_news_sm")

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
# Detecta nomes próprios comuns nos documentos
#NOMES_PESSOAS = ["lisa", "rick", "felipe", "sanders", "gavin", "jennifer"]
def detectar_pessoa_spacy(texto: str):
    """Detecta automaticamente nomes de pessoas em PT/EN usando spaCy."""
    if not texto:
        return []
    pessoas = set()

    # Análise em ambos os idiomas
    for nlp in [nlp_en, nlp_pt]:
        doc = nlp(texto)
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                pessoas.add(ent.text.strip())

    return list(pessoas)

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
# ============================================================
# 📁 Listagem de arquivos (com expansão bilíngue + paginação)
# ============================================================
@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    """
    Lista todos os arquivos do Google Drive (com paginação).
    - Expande automaticamente termos bilíngues.
    - Percorre todas as páginas (sem limite de 100 arquivos).
    - Retorna lista completa com metadados.
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
                    break  # ✅ todas as páginas lidas

        return {"arquivos": arquivos_encontrados, "total": len(arquivos_encontrados)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos: {e}")


@app.get("/smart_read")
def smart_read(file_id: str, query: str):
    """
    Busca leve dentro de um PowerPoint (.pptx) no Google Drive.
    Agora faz download e leitura em chunks, evitando carregar tudo em memória.
    """
    try:
        from pptx import Presentation
        import re, tempfile, os

        if not query:
            raise HTTPException(status_code=400, detail="Parâmetro 'query' é obrigatório.")

        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        nome = file["name"]
        mime = file["mimeType"]

        if mime not in [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.ms-powerpoint"
        ]:
            raise HTTPException(status_code=400, detail="O arquivo não é um PowerPoint (.pptx).")

        # 📦 Download com chunks
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pptx") as tmp:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(tmp, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Baixando {int(status.progress() * 100)}% de {nome}")
            tmp_path = tmp.name

        prs = Presentation(tmp_path)
        os.remove(tmp_path)

        slides = []
        for i, slide in enumerate(prs.slides, start=1):
            textos = []
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    textos.append(shape.text.strip())
            full_text = re.sub(r"\s{2,}", " ", " ".join(textos).strip())
            titulo = textos[0] if textos else f"Slide {i}"
            slides.append({"slide_numero": i, "titulo": titulo, "conteudo": full_text})

        query_regex = re.compile(re.escape(query), re.IGNORECASE)
        resultados = [s for s in slides if query_regex.search(s["conteudo"])]

        return {
            "arquivo": nome,
            "query": query,
            "total_slides": len(slides),
            "slides_encontrados": len(resultados),
            "resultados": resultados[:10],
        } if resultados else {
            "arquivo": nome,
            "query": query,
            "mensagem": "Nenhum slide contém o termo buscado.",
            "total_slides": len(slides),
            "resultados": []
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar smart_read: {e}")

@app.get("/smart_search")
def smart_search(query: str):
    """
    Realiza uma busca expandida:
    - Foco inicial em arquivos relacionados a uma pessoa específica (se aplicável)
    - Expansão semântica multilíngue caso nenhum resultado direto seja encontrado
    """
    try:
        service = get_service()
        termos = expandir_termos(query)
        pessoas = detectar_pessoa_spacy(query)
        pessoa = pessoas[0].lower() if pessoas else None
        arquivos_final = []
        ids_vistos = set()

        def buscar(termos_busca, foco_pessoa=False):
            encontrados = []
            for termo in termos_busca:
                q = f"name contains '{termo}' and trashed=false"
                results = service.files().list(
                    q=q,
                    fields="files(id, name, mimeType, modifiedTime)",
                    pageSize=100
                ).execute()
                for f in results.get("files", []):
                    if f["id"] not in ids_vistos:
                        try:
                            conteudo = ler_arquivo(f["id"])["conteudo"].lower()
                            # 🔍 filtro por pessoa, se houver
                            if foco_pessoa and pessoa not in f["name"].lower() and pessoa not in conteudo:
                                continue
                            # 🔍 busca textual
                            if any(t in conteudo for t in termos_busca):
                                encontrados.append(f)
                                ids_vistos.add(f["id"])
                        except Exception as err:
                            print(f"Erro ao ler {f['name']}: {err}")
                            continue
            return encontrados

        # 🔹 Nível 1: busca restrita à pessoa
        if pessoa:
            arquivos_final = buscar(termos, foco_pessoa=True)

        # 🔹 Nível 2: expansão se nada for encontrado
        expanded = False
        if not arquivos_final:
            arquivos_final = buscar(termos, foco_pessoa=False)
            expanded = True if pessoa else False

        return {
            "query_original": query,
            "pessoa_detectada": pessoa,
            "busca_expandida": expanded,
            "termos_expandidos": termos,
            "arquivos_encontrados": arquivos_final,
            "total": len(arquivos_final)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na busca expandida: {e}")

# ============================================================
# 📄 Função utilitária: leitura em blocos de texto
# ============================================================

def read_in_chunks(file_obj, chunk_size=8192):
    """Gerador que lê arquivos em blocos pequenos."""
    while True:
        data = file_obj.read(chunk_size)
        if not data:
            break
        yield data
# ============================================================
# 📄 Leitura e extração de conteúdo
# ============================================================
# ============================================================
# 📄 Leitura e extração de conteúdo (DOCX, PDF, TXT, PPTX)
# ============================================================
@app.get("/files/{file_id}")
def ler_arquivo(file_id: str, range_inicio: int = 1, range_fim: int = 15):
    """
    Faz download e leitura de arquivos do Google Drive (DOCX, PDF, TXT, PPTX)
    com suporte a leitura por chunks, payload limitado e fallback fragmentado.
    """
    try:
        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        nome = file["name"]
        mime = file["mimeType"]

        # 📦 Download seguro em chunks
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(nome)[-1]) as tmp_file:
            request = service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(tmp_file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Baixando {int(status.progress() * 100)}% de {nome}")
            temp_path = tmp_file.name

        texto_extraido = ""
        total_paginas = 0

        # ------------------------------------------------------------
        # DOCX
        # ------------------------------------------------------------
        if mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            texto_extraido = docx2txt.process(temp_path)

        # ------------------------------------------------------------
        # PDF (em blocos de páginas)
        # ------------------------------------------------------------
        elif mime == "application/pdf":
            with open(temp_path, "rb") as f:
                reader = PdfReader(f)
                total_paginas = len(reader.pages)
                blocos = []
                for i, page in enumerate(reader.pages, start=1):
                    texto = page.extract_text() or ""
                    blocos.append(texto)
                    # Interrompe e envia resultado parcial se exceder o limite
                    if len("".join(blocos)) > MAX_PAYLOAD:
                        break
                texto_extraido = "\n".join(blocos)

        # ------------------------------------------------------------
        # TXT (stream de leitura incremental)
        # ------------------------------------------------------------
        elif "text" in mime:
            with open(temp_path, "r", encoding="utf-8", errors="ignore") as f:
                partes = []
                for chunk in read_in_chunks(f, 8192):
                    partes.append(chunk)
                    if sum(len(p) for p in partes) > MAX_PAYLOAD:
                        break
                texto_extraido = "".join(partes)

        # ------------------------------------------------------------
        # PPTX (leitura parcial em slides)
        # ------------------------------------------------------------
        elif mime in [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.ms-powerpoint"
        ]:
            prs = Presentation(temp_path)
            total_slides = len(prs.slides)
            range_inicio = max(1, min(range_inicio, total_slides))
            range_fim = max(range_inicio, min(range_fim, total_slides))

            slides_texto = []
            for i in range(range_inicio, range_fim + 1):
                slide = prs.slides[i - 1]
                textos = []
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text.strip():
                        textos.append(shape.text.strip())
                slides_texto.append(f"[Slide {i}] {' '.join(textos)}")
                if len("\n".join(slides_texto)) > MAX_PAYLOAD:
                    break
            texto_extraido = "\n".join(slides_texto)

        else:
            texto_extraido = f"O tipo de arquivo {mime} não é suportado para leitura direta."

        os.remove(temp_path)

        # Sanitização final
        texto_extraido = re.sub(r"[\u0000-\u001F\u007F-\u009F]", " ", texto_extraido)
        texto_extraido = re.sub(r"\s{2,}", " ", texto_extraido).strip()

        if len(texto_extraido) > MAX_PAYLOAD:
            texto_extraido = texto_extraido[:MAX_PAYLOAD] + "\n\n[⚠️ Conteúdo truncado para compatibilidade de payload.]"

        if not texto_extraido.strip():
            texto_extraido = "⚠️ O arquivo foi encontrado, mas não contém texto legível."

        return {
            "nome": nome,
            "tipo": mime,
            "intervalo_lido": f"{range_inicio}-{range_fim}",
            "conteudo": texto_extraido
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler arquivo: {e}")


@app.get("/files/{file_id}/stream")
def stream_arquivo(file_id: str):
    """Retorna o conteúdo do arquivo em streaming incremental."""
    def iterar():
        service = get_service()
        file = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        nome = file["name"]
        request = service.files().get_media(fileId=file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(nome)[-1]) as tmp:
            downloader = MediaIoBaseDownload(tmp, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Baixando {int(status.progress() * 100)}%...")
            tmp.seek(0)
            for chunk in read_in_chunks(tmp, 8192):
                yield chunk
    return StreamingResponse(iterar(), media_type="text/plain")


@app.get("/files")
def listar_arquivos(pasta_id: str = None, query: str = None):
    try:
        service = get_service()
        q = []
        if pasta_id:
            q.append(f"'{pasta_id}' in parents")
        if query:
            q.append(f"name contains '{query.lower()}'")
        q.append("trashed=false")
        query_final = " and ".join(q)
        results = service.files().list(
            q=query_final,
            fields="files(id, name, mimeType, modifiedTime, parents)",
            pageSize=100
        ).execute()
        return {"arquivos": results.get("files", []), "total": len(results.get("files", []))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar arquivos: {e}")


@app.get("/index_drive")
def indexar_drive(pasta_raiz: str = None):
    """
    🔄 Indexa o conteúdo do Google Drive (recursivamente) com paginação em chunks.
    - Armazena resultados parciais a cada 500 arquivos (checkpoints).
    - Evita manter o índice inteiro em memória.
    """
    try:
        service = get_service()
        arquivos_indexados = []
        pastas_a_visitar = [pasta_raiz or "root"]
        total_processados = 0

        os.makedirs("index_cache", exist_ok=True)
        index_path = "index_cache/drive_index.json"

        def salvar_checkpoint():
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump({
                    "timestamp": datetime.utcnow().isoformat(),
                    "total_itens": total_processados,
                    "arquivos": arquivos_indexados[-500:]
                }, f, ensure_ascii=False, indent=2)

        def listar_conteudo(pasta_id, caminho_atual=""):
            nonlocal total_processados
            page_token = None
            while True:
                results = service.files().list(
                    q=f"'{pasta_id}' in parents and trashed=false",
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, parents)",
                    pageSize=100,
                    pageToken=page_token
                ).execute()

                for item in results.get("files", []):
                    caminho = f"{caminho_atual}/{item['name']}".strip("/")
                    item["path"] = caminho
                    arquivos_indexados.append(item)
                    total_processados += 1

                    # Salva checkpoints a cada 500 arquivos
                    if total_processados % 500 == 0:
                        salvar_checkpoint()

                    if item["mimeType"] == "application/vnd.google-apps.folder":
                        listar_conteudo(item["id"], caminho)

                page_token = results.get("nextPageToken")
                if not page_token:
                    break

        for pasta_id in pastas_a_visitar:
            listar_conteudo(pasta_id)

        salvar_checkpoint()

        return {
            "status": "✅ Indexação concluída com sucesso",
            "total_arquivos": total_processados,
            "index_path": index_path
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao indexar o Drive: {e}")

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
