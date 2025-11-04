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
def ler_arquivo(file_id: str):
    """Faz download e extrai texto de arquivos do Google Drive (.docx, .pdf, .txt, .pptx)."""
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
        # 🖼️ PPTX (PowerPoint) — leitura hierárquica com posição e estrutura
        # --------------------------------------------------------
        elif mime in [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.ms-powerpoint"
        ]:
            from pptx import Presentation

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pptx") as temp_file:
                temp_file.write(fh.read())
                temp_path = temp_file.name

            prs = Presentation(temp_path)
            slides_estruturados = []

            def extrair_texto_recursivo(shape, elementos):
                """Extrai texto e posição de shapes, incluindo grupos e tabelas."""
                if hasattr(shape, "text") and shape.text.strip():
                    elementos.append({
                        "texto": shape.text.strip(),
                        "x": int(getattr(shape, "left", 0)),
                        "y": int(getattr(shape, "top", 0)),
                        "largura": int(getattr(shape, "width", 0)),
                        "altura": int(getattr(shape, "height", 0)),
                    })
                # Percorre subshapes (grupos)
                if hasattr(shape, "shapes"):
                    for subshape in shape.shapes:
                        extrair_texto_recursivo(subshape, elementos)
                # Lê tabelas como texto concatenado
                if hasattr(shape, "has_table") and shape.has_table:
                    table = shape.table
                    for row in table.rows:
                        row_text = " | ".join(cell.text.strip() for cell in row.cells if cell.text.strip())
                        if row_text:
                            elementos.append({
                                "texto": row_text,
                                "x": int(getattr(shape, "left", 0)),
                                "y": int(getattr(shape, "top", 0)),
                                "largura": int(getattr(shape, "width", 0)),
                                "altura": int(getattr(shape, "height", 0)),
                            })

            for i, slide in enumerate(prs.slides, start=1):
                elementos = []

                for shape in slide.shapes:
                    extrair_texto_recursivo(shape, elementos)

                # Agrupa por "linha visual" (faixas horizontais)
                elementos_ordenados = sorted(elementos, key=lambda e: (e["y"], e["x"]))

                linha_id = 0
                ultima_y = None
                for el in elementos_ordenados:
                    if ultima_y is None or abs(el["y"] - ultima_y) > 200000:  # espaçamento entre faixas
                        linha_id += 1
                        ultima_y = el["y"]
                    el["linha_visual"] = linha_id

                # Coleta notas do apresentador (se houver)
                notas = None
                if slide.has_notes_slide and slide.notes_slide.notes_text_frame:
                    notas = slide.notes_slide.notes_text_frame.text.strip()

                # Captura possível título
                titulo_slide = next((e["texto"] for e in elementos_ordenados if e["linha_visual"] == 1), f"Slide {i}")

                slides_estruturados.append({
                    "slide_numero": i,
                    "titulo": titulo_slide,
                    "elementos": elementos_ordenados,
                    "notas": notas
                })

            # 🔄 Monta o conteúdo final
            texto_extraido = ""
            for s in slides_estruturados:
                # 🔄 Monta o conteúdo final legível e estruturado
                texto_extraido = ""
                for s in slides_estruturados:
                    texto_extraido += f"\n\n=== SLIDE {s['slide_numero']} - {s['titulo']} ===\n"
                    for e in s["elementos"]:
                        texto_extraido += f"[linha {e['linha_visual']}] {e['texto']}\n"

                # 🧹 Limpeza e normalização mais profunda
                texto_extraido = (
                    texto_extraido
                    .replace("\\n", "\n")
                    .replace("\r", "\n")
                    .replace("\t", " ")
                )
                texto_extraido = re.sub(r"[\u0000-\u001F\u007F-\u009F]", " ",
                                        texto_extraido)  # remove caracteres invisíveis
                texto_extraido = re.sub(r"\u00A0", " ", texto_extraido)  # espaço não separável
                texto_extraido = re.sub(r"\s{2,}", " ", texto_extraido)  # normaliza espaços duplos
                texto_extraido = re.sub(r"\n{2,}", "\n", texto_extraido).strip()

                os.remove(temp_path)

                # ✅ Retorna conteúdo híbrido (legível + estruturado)
                return {
                    "nome": nome,
                    "tipo": mime,
                    "conteudo": texto_extraido[:80000],  # texto limpo e pronto p/ análise GPT
                    "conteudo_estruturado": slides_estruturados  # metadados com X/Y, linha_visual etc.
                }






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
    return {"message": "✅ AIDA Drive Connector RAG (multilíngue) está ativo e pronto para uso."}

if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.getenv("PORT", 8080))  # 👈 Render injeta a variável PORT
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
