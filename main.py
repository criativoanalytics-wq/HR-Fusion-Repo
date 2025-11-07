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
import re
import spacy
import json
from datetime import datetime

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
    Busca leve dentro de um PPTX do Google Drive.
    Retorna apenas os slides que contêm o termo especificado.
    Ideal para agentes com limite de payload.
    """
    try:
        from pptx import Presentation
        from io import BytesIO
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

        # 📦 Download temporário
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pptx") as tmp:
            tmp.write(fh.read())
            tmp_path = tmp.name

        prs = Presentation(tmp_path)
        os.remove(tmp_path)

        slides = []
        for i, slide in enumerate(prs.slides, start=1):
            textos = []
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    textos.append(shape.text.strip())
            full_text = " ".join(textos)
            full_text = re.sub(r"\s{2,}", " ", full_text).strip()

            titulo = textos[0] if textos else f"Slide {i}"
            slides.append({
                "slide_numero": i,
                "titulo": titulo,
                "conteudo": full_text
            })

        # 🔍 Busca simples (case-insensitive)
        query_regex = re.compile(re.escape(query), re.IGNORECASE)
        resultados = [
            s for s in slides
            if query_regex.search(s["conteudo"])
        ]

        if not resultados:
            return {
                "arquivo": nome,
                "query": query,
                "total_slides": len(slides),
                "resultados": [],
                "mensagem": "Nenhum slide contém o termo buscado."
            }

        # 🔎 Resumo dos resultados (leve para GPT)
        return {
            "arquivo": nome,
            "query": query,
            "total_slides": len(slides),
            "slides_encontrados": len(resultados),
            "resultados": resultados[:10]  # limite de segurança
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
# 📄 Leitura e extração de conteúdo
# ============================================================
# ============================================================
# 📄 Leitura e extração de conteúdo (DOCX, PDF, TXT, PPTX)
# ============================================================
@app.get("/files/{file_id}")
def ler_arquivo(file_id: str, range_inicio: int = 1, range_fim: int = 15):
    """
    Faz download e extrai texto de arquivos (.docx, .pdf, .txt, .pptx),
    com suporte a leitura paginada e tolerância a falhas.
    """
    import tempfile as tmp
    import re, os
    from pptx import Presentation

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
            try:
                with tmp.NamedTemporaryFile(delete=False, suffix=".docx") as temp_file:
                    temp_file.write(fh.read())
                    temp_path = temp_file.name
                texto_extraido = docx2txt.process(temp_path)
            finally:
                if os.path.exists(temp_path):
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
        # 🖼️ PPTX (PowerPoint) — leitura hierárquica com paginação
        # --------------------------------------------------------
        elif mime in [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.ms-powerpoint"
        ]:
            try:
                with tmp.NamedTemporaryFile(delete=False, suffix=".pptx") as temp_file:
                    temp_file.write(fh.read())
                    temp_path = temp_file.name

                prs = Presentation(temp_path)
                total_slides = len(prs.slides)
                range_inicio = max(1, min(range_inicio, total_slides))
                range_fim = max(range_inicio, min(range_fim, total_slides))

                slides_estruturados = []

                def extrair_texto_recursivo(shape, elementos):
                    if hasattr(shape, "text") and shape.text.strip():
                        elementos.append({
                            "texto": shape.text.strip(),
                            "x": int(getattr(shape, "left", 0)),
                            "y": int(getattr(shape, "top", 0))
                        })
                    if hasattr(shape, "shapes"):
                        for subshape in shape.shapes:
                            extrair_texto_recursivo(subshape, elementos)
                    if hasattr(shape, "has_table") and shape.has_table:
                        table = shape.table
                        for row in table.rows:
                            row_text = " | ".join(
                                cell.text.strip() for cell in row.cells if cell.text.strip()
                            )
                            if row_text:
                                elementos.append({
                                    "texto": row_text,
                                    "x": int(getattr(shape, "left", 0)),
                                    "y": int(getattr(shape, "top", 0))
                                })

                for i in range(range_inicio, range_fim + 1):
                    slide = prs.slides[i - 1]
                    elementos = []
                    for shape in slide.shapes:
                        extrair_texto_recursivo(shape, elementos)

                    elementos_ordenados = sorted(elementos, key=lambda e: (e["y"], e["x"]))
                    linha_id, ultima_y = 0, None
                    for el in elementos_ordenados:
                        if ultima_y is None or abs(el["y"] - ultima_y) > 200000:
                            linha_id += 1
                            ultima_y = el["y"]
                        el["linha_visual"] = linha_id

                    faixas = {}
                    for e in elementos_ordenados:
                        faixa_id = e["linha_visual"]
                        faixas.setdefault(faixa_id, []).append(e["texto"])
                    faixas_agrupadas = [
                        {"linha_visual": k, "conteudo": " ".join(v)} for k, v in faixas.items()
                    ]

                    notas = ""
                    if slide.has_notes_slide and slide.notes_slide.notes_text_frame:
                        notas = slide.notes_slide.notes_text_frame.text.strip()

                    titulo_slide = next(
                        (e["texto"] for e in elementos_ordenados if e["linha_visual"] == 1),
                        f"Slide {i}"
                    )

                    slides_estruturados.append({
                        "slide_numero": i,
                        "titulo": titulo_slide,
                        "faixas": faixas_agrupadas,
                        "notas": notas
                    })

                texto_extraido = ""
                for s in slides_estruturados:
                    texto_extraido += f"\n\n=== SLIDE {s['slide_numero']} - {s['titulo']} ===\n"
                    for f in s["faixas"]:
                        texto_extraido += f"[Faixa {f['linha_visual']}] {f['conteudo']}\n"

                texto_extraido = (
                    texto_extraido.replace("\\n", "\n")
                    .replace("\r", "\n")
                    .replace("\t", " ")
                )
                texto_extraido = re.sub(r"[\u0000-\u001F\u007F-\u009F]", " ", texto_extraido)
                texto_extraido = re.sub(r"\u00A0", " ", texto_extraido)
                texto_extraido = re.sub(r"\s{2,}", " ", texto_extraido)
                texto_extraido = re.sub(r"\n{2,}", "\n", texto_extraido).strip()

            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)

            return {
                "nome": nome,
                "tipo": mime,
                "intervalo_lido": f"{range_inicio}-{range_fim}",
                "total_slides": total_slides,
                "conteudo": texto_extraido[:80000],
                "conteudo_estruturado": slides_estruturados
            }

        # --------------------------------------------------------
        # ❗ Outros formatos
        # --------------------------------------------------------
        else:
            texto_extraido = f"O tipo de arquivo {mime} não é suportado para leitura direta."

        if not isinstance(texto_extraido, str) or not texto_extraido.strip():
            texto_extraido = "⚠️ O arquivo foi encontrado, mas parece não conter texto legível."

        return {
            "nome": nome,
            "tipo": mime,
            "conteudo": texto_extraido[:50000]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler arquivo: {e}")

@app.get("/index_drive")
def indexar_drive(pasta_raiz: str = None):
    """
    🔄 Indexa todo o conteúdo do Google Drive (recursivamente).
    - Lista todas as pastas e arquivos com paginação.
    - Armazena metadados em drive_index.json para uso rápido.
    """
    try:
        service = get_service()
        arquivos_indexados = []
        pastas_a_visitar = [pasta_raiz] if pasta_raiz else [ "root" ]

        def listar_conteudo(pasta_id, caminho_atual=""):
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

                    # Se for pasta → adiciona à fila
                    if item["mimeType"] == "application/vnd.google-apps.folder":
                        listar_conteudo(item["id"], caminho)

                page_token = results.get("nextPageToken")
                if not page_token:
                    break

        # 🔁 Inicia varredura
        for pasta_id in pastas_a_visitar:
            listar_conteudo(pasta_id)

        # 📦 Cria diretório local para índice
        os.makedirs("index_cache", exist_ok=True)
        index_path = f"index_cache/drive_index.json"

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": datetime.utcnow().isoformat(),
                "total_itens": len(arquivos_indexados),
                "arquivos": arquivos_indexados
            }, f, ensure_ascii=False, indent=2)

        return {
            "status": "✅ Indexação concluída com sucesso",
            "total_arquivos": len(arquivos_indexados),
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
