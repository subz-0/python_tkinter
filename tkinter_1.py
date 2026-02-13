import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
import tkinter.font as tkfont
import sqlite3
import pandas as pd
import json
import os
from datetime import datetime
import getpass
import traceback
import shutil
import ast
import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt
import threading
import subprocess
import sys

# optional interactivity
try:
    import mplcursors
    MPLCURSORS_AVAILABLE = True
except Exception:
    MPLCURSORS_AVAILABLE = False

# ---------------------------
# Constantes / diretórios
# ---------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "Data")

DEFAULT_DB = os.path.join(DATA_DIR, "database.db")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
EXPORT_DIR = os.path.join(DATA_DIR, "exportacoes")
BACKUP_DIR = os.path.join(DATA_DIR, "Backups")
LOGS_DIR = os.path.join(DATA_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

try:
    CURRENT_USER = getpass.getuser()
except Exception:
    CURRENT_USER = "unknown"

# ---------------------------
# Config (persistente em JSON)
# ---------------------------
class Config:
    def __init__(self):
        self.visual_cols = {}
        self.report_cols = []
        self.col_types = {}
        # col_standardization: {table_or_*: {col: {"mode":"free"|"fixed", "values": [], "required": False}}}
        self.col_standardization = {}
        self.db_path = ""
        self.load()

    def get_visual(self, table, allcols):
        v = self.visual_cols.get(table, None)
        if v is None:
            v = self.visual_cols.get("*", None)
        return v if v is not None else allcols

    def set_visual(self, table, cols):
        self.visual_cols[table] = cols
        self.save()

    def get_report(self, allcols):
        return self.report_cols if self.report_cols else allcols

    def set_report(self, cols):
        self.report_cols = cols
        self.save()

    def get_col_type(self, table, col):
        if table in self.col_types and col in self.col_types[table]:
            return self.col_types[table][col]
        if "*" in self.col_types and col in self.col_types["*"]:
            return self.col_types["*"][col]
        return "text"

    def set_col_type(self, table, col, type_str):
        if table not in self.col_types:
            self.col_types[table] = {}
        self.col_types[table][col] = type_str
        self.save()

    def get_col_standardization(self, table, col):
        # precedence: specific table, then "*"
        if table in self.col_standardization and col in self.col_standardization[table]:
            std = self.col_standardization[table][col]
            return {"mode": std.get("mode", "free"), "values": std.get("values", []), "required": std.get("required", False)}
        if "*" in self.col_standardization and col in self.col_standardization["*"]:
            std = self.col_standardization["*"][col]
            return {"mode": std.get("mode", "free"), "values": std.get("values", []), "required": std.get("required", False)}
        return {"mode": "free", "values": [], "required": False}

    def set_col_standardization(self, table, col, mode, values, required=False):
        if table not in self.col_standardization:
            self.col_standardization[table] = {}
        self.col_standardization[table][col] = {"mode": mode, "values": list(values), "required": bool(required)}
        self.save()

    def set_db_path(self, path):
        self.db_path = path
        self.save()

    def save(self):
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "visual_cols": self.visual_cols,
                    "report_cols": self.report_cols,
                    "col_types": self.col_types,
                    "col_standardization": self.col_standardization,
                    "db_path": self.db_path
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print("Erro ao salvar settings:", e)

    def load(self):
        if os.path.isfile(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.visual_cols = data.get("visual_cols", {})
                    self.report_cols = data.get("report_cols", [])
                    self.col_types = data.get("col_types", {})
                    self.col_standardization = data.get("col_standardization", {})
                    self.db_path = data.get("db_path", "")
            except Exception as e:
                print("Erro ao carregar settings:", e)

config = Config()

# ---------------------------
# DB file a usar (pode ser alterado pelo usuário)
# ---------------------------
DB_FILE = config.db_path if config.db_path else DEFAULT_DB

# ---------------------------
# Helpers DB / FS / logs / backup
# ---------------------------
def get_conn():
    return sqlite3.connect(DB_FILE)

def ensure_dirs_for_backup_and_logs():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)

def copy_db_backup():
    try:
        ensure_dirs_for_backup_and_logs()
        date_folder = datetime.now().strftime("%Y-%m-%d")
        folder = os.path.join(BACKUP_DIR, date_folder)
        os.makedirs(folder, exist_ok=True)
        tsfile = datetime.now().strftime("%H%M%S_%f")
        filename = f"backup dp {tsfile}.db"
        dst = os.path.join(folder, filename)
        if os.path.isfile(DB_FILE):
            shutil.copy2(DB_FILE, dst)
            return dst
    except Exception:
        print("Erro ao copiar backup do DB:", traceback.format_exc())
    return None

def write_log_file(payload):
    try:
        ensure_dirs_for_backup_and_logs()
        date_folder = datetime.now().strftime("%Y-%m-%d")
        folder = os.path.join(LOGS_DIR, date_folder)
        os.makedirs(folder, exist_ok=True)
        tsfile = datetime.now().strftime("%H%M%S_%f")
        filename = f"{tsfile}.json"
        path = os.path.join(folder, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path
    except Exception:
        print("Erro ao gravar log:", traceback.format_exc())
        return None

def mirror_db_to_excel():
    """
    Exports each table to its own sheet and also a 'geral' sheet with all rows combined.
    """
    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        tables = listar_tabelas()
        outpath = os.path.join(EXPORT_DIR, "db_excel.xlsx")
        with pd.ExcelWriter(outpath) as writer:
            all_dfs = []
            for t in tables:
                try:
                    df = fetch_table(t)
                    # write table sheet
                    sheet_name = t[:31] if t else "sheet"
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    if not df.empty:
                        # add column indicating origem table to geral
                        dft = df.copy()
                        dft["__tabela_origem"] = t
                        all_dfs.append(dft)
                except Exception:
                    pd.DataFrame({"error": [f"falha ao exportar tabela {t}"]}).to_excel(writer, sheet_name=(t[:31] if t else "sheet"), index=False)
            # write geral sheet
            if all_dfs:
                geral = pd.concat(all_dfs, ignore_index=True)
            else:
                geral = pd.DataFrame()
            try:
                geral.to_excel(writer, sheet_name="geral", index=False)
            except Exception:
                pass
    except Exception:
        print("Erro ao gerar mirror excel:", traceback.format_exc())

def mirror_db_to_excel_only_geral():
    """
    Exports all tables consolidated into a single 'geral' sheet
    directly into the Data directory.
    """
    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        tables = listar_tabelas()
        outpath = os.path.join(DATA_DIR, "db_excel.xlsx")

        all_dfs = []

        for t in tables:
            try:
                df = fetch_table(t)
                if not df.empty:
                    dft = df.copy()
                    dft["__tabela_origem"] = t
                    all_dfs.append(dft)
            except Exception:
                # ignora erro de tabela individual
                continue

        if all_dfs:
            geral = pd.concat(all_dfs, ignore_index=True)
        else:
            geral = pd.DataFrame()

        with pd.ExcelWriter(outpath) as writer:
            geral.to_excel(writer, sheet_name="geral", index=False)

    except Exception:
        print("Erro ao gerar mirror excel:", traceback.format_exc())


# ---------------------------
# DB helpers
# ---------------------------
def listar_tabelas():
    try:
        con = get_conn()
    except Exception:
        return []
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tabelas = [row[0] for row in cur.fetchall()]
    con.close()
    return tabelas

def get_table_columns(tablename):
    try:
        con = get_conn()
    except Exception:
        return []
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info(\"{tablename}\")")
    columns = [row[1] for row in cur.fetchall()]
    con.close()
    return columns

def fetch_table(tablename):
    try:
        con = get_conn()
        df = pd.read_sql(f"SELECT * FROM \"{tablename}\"", con)
        con.close()
        return df
    except Exception:
        return pd.DataFrame()

def table_has_id(tablename, idval):
    try:
        con = get_conn()
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(1) FROM \"{tablename}\" WHERE id=?", (idval,))
        res = cur.fetchone()[0]
        con.close()
        return res > 0
    except Exception:
        return False

def update_cell(tablename, col, val, rowid):
    con = get_conn()
    cur = con.cursor()
    try:
        cur.execute(f"SELECT {col} FROM \"{tablename}\" WHERE id=?", (rowid,))
        old = cur.fetchone()
        oldval = old[0] if old is not None else None
        cur.execute(f"UPDATE \"{tablename}\" SET {col}=? WHERE id=?", (val, rowid))
        con.commit()
        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tabela": tablename,
            "rowid": str(rowid),
            "coluna": col,
            "valor_antigo": None if oldval is None else str(oldval),
            "valor_novo": None if val is None else str(val),
            "usuario": CURRENT_USER,
            "acao": "UPDATE"
        }
        write_log_file(payload)
        #copy_db_backup()
        mirror_db_to_excel()
    finally:
        con.close()

def insert_row(tablename, values_dict):
    con = get_conn()
    cur = con.cursor()
    try:
        idval = values_dict.get("id")
        if idval:
            cur.execute(f"SELECT COUNT(1) FROM \"{tablename}\" WHERE id=?", (idval,))
            if cur.fetchone()[0] > 0:
                raise ValueError(f"Já existe um registro com id={idval} na tabela {tablename}.")
        cols = list(values_dict.keys())
        vals = [values_dict[c] for c in cols]
        placeholders = ", ".join(["?" for _ in cols])
        colstr = ", ".join([f'"{c}"' for c in cols])
        cur.execute(f"INSERT INTO \"{tablename}\" ({colstr}) VALUES ({placeholders})", vals)
        con.commit()
        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tabela": tablename,
            "rowid": str(idval),
            "acao": "INSERT",
            "detalhes": values_dict,
            "usuario": CURRENT_USER
        }
        write_log_file(payload)
        #copy_db_backup()
        mirror_db_to_excel()
        return idval
    finally:
        con.close()

def delete_row(tablename, rowid):
    con = get_conn()
    cur = con.cursor()
    try:
        try:
            rowid_int = int(rowid)
        except Exception:
            rowid_int = rowid
        cur.execute(f"SELECT * FROM \"{tablename}\" WHERE id=?", (rowid_int,))
        result = cur.fetchone()
        if result:
            cols = get_table_columns(tablename)
            rowdict = {cols[idx]: result[idx] for idx in range(len(cols))}
            payload = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "tabela": tablename,
                "rowid": str(rowid_int),
                "acao": "DELETE",
                "valor_antigo": rowdict,
                "usuario": CURRENT_USER
            }
            write_log_file(payload)
        cur.execute(f"DELETE FROM \"{tablename}\" WHERE id=?", (rowid_int,))
        con.commit()
        #copy_db_backup()
        mirror_db_to_excel()
    finally:
        con.close()

def drop_table(tablename):
    con = get_conn()
    cur = con.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS \"{tablename}\"")
        con.commit()
        if tablename in config.visual_cols:
            del config.visual_cols[tablename]
        if tablename in config.col_types:
            del config.col_types[tablename]
        if tablename in config.col_standardization:
            del config.col_standardization[tablename]
        config.save()
        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tabela": tablename,
            "acao": "DROP_TABLE",
            "usuario": CURRENT_USER
        }
        write_log_file(payload)
        #copy_db_backup()
        mirror_db_to_excel()
    finally:
        con.close()

def criar_tabela_padrao(nome):
    con = get_conn()
    cur = con.cursor()
    sql = f"""CREATE TABLE IF NOT EXISTS "{nome}" (
        id TEXT PRIMARY KEY,
        documento TEXT,
        empresa TEXT,
        valor_adquirido REAL,
        numero_parcelas INTEGER,
        data_inicio TEXT,
        data_fim TEXT,
        juros_anual REAL,
        juros_mensal REAL,
        parcela_automatica TEXT,
        parcelas TEXT,
        tipo_calculo INTEGER,
        tipo_parcela INTEGER,
        saldo_devedor REAL,
        saldo_devedor_com_juros REAL,
        tuplas TEXT
    )"""
    cur.execute(sql)
    con.commit()
    con.close()

# ---------------------------
# Import / Export
# ---------------------------
def ensure_export_dir():
    os.makedirs(EXPORT_DIR, exist_ok=True)

def export_dataframe(df, formats, base_folder_name=None, filename_base="report"):
    ensure_export_dir()
    tsfolder = base_folder_name if base_folder_name else datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outdir = os.path.join(EXPORT_DIR, tsfolder)
    os.makedirs(outdir, exist_ok=True)
    results = []
    if df is None or df.empty:
        return {"ok": False, "message": "DataFrame vazio"}
    if "csv" in formats:
        path = os.path.join(outdir, f"{filename_base}.csv")
        df.to_csv(path, index=False, encoding="utf-8")
        results.append(path)
    if "json" in formats:
        path = os.path.join(outdir, f"{filename_base}.json")
        df.to_json(path, orient="records", force_ascii=False, date_unit="s")
        results.append(path)
    if "excel" in formats:
        path = os.path.join(outdir, f"{filename_base}.xlsx")
        try:
            df.to_excel(path, index=False)
            results.append(path)
        except Exception:
            path2 = os.path.join(outdir, f"{filename_base}.txt")
            with open(path2, "w", encoding="utf-8") as f:
                f.write(df.to_string(index=False))
            results.append(path2)
    return {"ok": True, "paths": results, "folder": outdir}

def import_file_to_table(filepath, tablename):
    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext in [".csv", ".txt"]:
            df = pd.read_csv(filepath, dtype=str)
        elif ext in [".json"]:
            df = pd.read_json(filepath, dtype=str)
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(filepath, dtype=str)
        else:
            return {"ok": False, "message": "Formato não suportado"}
    except Exception as e:
        return {"ok": False, "message": f"Erro ao ler arquivo: {e}"}
    cols = get_table_columns(tablename)
    con = get_conn()
    cur = con.cursor()
    inserted = 0
    updated = 0
    try:
        for _, row in df.iterrows():
            rowdict = {c: (str(row.get(c, "")) if c in row.index else "") for c in cols}
            idval = rowdict.get("id")
            if idval:
                cur.execute(f"SELECT COUNT(*) FROM \"{tablename}\" WHERE id=?", (idval,))
                exists = cur.fetchone()[0] > 0
                if exists:
                    set_clause = ", ".join([f"{c}=?" for c in cols if c != "id"])
                    vals = [rowdict[c] for c in cols if c != "id"] + [idval]
                    cur.execute(f"UPDATE \"{tablename}\" SET {set_clause} WHERE id=?", vals)
                    updated += 1
                    payload = {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "acao": "IMPORT_UPDATE",
                        "tabela": tablename,
                        "rowid": idval,
                        "detalhes": rowdict,
                        "usuario": CURRENT_USER
                    }
                    write_log_file(payload)
                else:
                    placeholders = ", ".join(["?" for _ in cols])
                    colstr = ", ".join([f'"{c}"' for c in cols])
                    vals = [rowdict[c] for c in cols]
                    cur.execute(f"INSERT INTO \"{tablename}\" ({colstr}) VALUES ({placeholders})", vals)
                    inserted += 1
                    payload = {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "acao": "IMPORT_INSERT",
                        "tabela": tablename,
                        "rowid": idval,
                        "detalhes": rowdict,
                        "usuario": CURRENT_USER
                    }
                    write_log_file(payload)
            else:
                placeholders = ", ".join(["?" for _ in cols])
                colstr = ", ".join([f'"{c}"' for c in cols])
                vals = [rowdict[c] for c in cols]
                cur.execute(f"INSERT INTO \"{tablename}\" ({colstr}) VALUES ({placeholders})", vals)
                inserted += 1
                payload = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "acao": "IMPORT_INSERT",
                    "tabela": tablename,
                    "rowid": rowdict.get("id"),
                    "detalhes": rowdict,
                    "usuario": CURRENT_USER
                }
                write_log_file(payload)
        con.commit()
        #copy_db_backup()
        mirror_db_to_excel()
    except Exception as e:
        con.rollback()
        return {"ok": False, "message": f"Erro ao importar: {e}"}
    finally:
        con.close()
    return {"ok": True, "inserted": inserted, "updated": updated}

# ---------------------------
# GUI Helpers: style + fonts
# ---------------------------
def apply_style(root):
    style = ttk.Style(root)
    try:
        style.theme_use("vista")
    except Exception:
        try:
            style.theme_use("clam")
        except Exception:
            pass
    style.configure("TButton", padding=6, font=("Segoe UI", 10))
    style.configure("Primary.TButton", background="#2b7de9", foreground="white", font=("Segoe UI", 10, "bold"))
    style.configure("TLabel", font=("Segoe UI", 10))
    style.configure("Header.TLabel", font=("Segoe UI", 15, "bold"))
    style.configure("Card.TFrame", background="#ffffff", relief="flat")
    style.map("Primary.TButton", background=[('active', '#1b66d1')])

# ---------------------------
# Application
# ---------------------------
class FinanceManagerGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Gestão Financeira - v2")
        self.geometry("1360x860")
        self.minsize(1100, 720)
        apply_style(self)
        self.font_header = tkfont.Font(family="Segoe UI", size=16, weight="bold")
        self.font_normal = tkfont.Font(family="Segoe UI", size=10)
        self.selected_tables = []
        self.selected_empresas = []
        self.check_vars = {}
        self.empresa_vars = {}
        self._last_report_df = pd.DataFrame()
        self._active_canvas_bindings = []
        self._graph_canvas = None
        self._graph_toolbar = None
        self._update_button = None  # reference to Atualizar button
        self._build_ui()
        self.show_home()

    def _build_ui(self):
        topbar = ttk.Frame(self, padding=(8,8))
        topbar.pack(side="top", fill="x")
        title = ttk.Label(topbar, text="Gestão Financeira", style="Header.TLabel")
        title.pack(side="left", padx=(6,12))

        btns = ttk.Frame(topbar)
        btns.pack(side="left")
        for text, cmd in [
            ("Início", self.show_home),
            ("Tabelas", self.show_tables),
            ("Importar", self.show_import_dialog),
            ("Exportar", self.show_export_dialog),
            ("Gráficos", self.show_graphs),
            ("Relatórios", self.show_reports),
            ("Configurações", self.show_config),
            ("Selecionar DB", self.select_db_dialog),
        ]:
            ttk.Button(
                btns,
                text=text,
                command=cmd,
                style="TButton"
            ).pack(side="left", padx=4)

        # NEW: Atualizar button
        self._update_button = ttk.Button(btns, text="Atualizar", command=self.run_update_script)
        self._update_button.pack(side="left", padx=6)

        userlbl = ttk.Label(topbar, text=f"Usuário: {CURRENT_USER}", font=("Segoe UI", 9, "italic"))
        userlbl.pack(side="right", padx=8)

        self.main_frame = ttk.Frame(self, padding=12)
        self.main_frame.pack(fill="both", expand=True)

    def clear_main(self):
        for w in self.main_frame.winfo_children():
            w.destroy()
        # unbind any canvas wheel bindings we created
        for b in list(self._active_canvas_bindings):
            try:
                self.unbind_all(b)
            except Exception:
                pass
        self._active_canvas_bindings.clear()




    def show_home(self):

        FIELD_DESCRIPTIONS = {
            "tabela": "Banco/tabela cujo pertence o registro em questão.",
            "id": "Identificador único do contrato ou operação. Não pode se repetir.",
            "documento": "Número do contrato ou documento de referência.",
            "empresa": "Empresa vinculada ao contrato ou operação financeira.",
            "valor_adquirido": "Valor original contratado ou financiado.",
            "numero_parcelas": "Quantidade total de parcelas do contrato.",
            "data_inicio": "Data de início do contrato (DD-MM-AAAA).",
            "data_fim": "Data prevista para encerramento do contrato.",
            "juros_anual": "Taxa de juros anual aplicada ao contrato.",
            "juros_mensal": "Taxa de juros mensal equivalente.",
            "parcela_automatica": "Indica se as parcelas são geradas automaticamente.",
            "parcelas": "Descritivo de cada parcela, não é um campo preenchivel.",
            "tipo_calculo": "Define a forma de cálculo dos juros.",
            "tipo_parcela": "Define se a periodicidade da parcela.",
            "saldo_devedor": "Saldo restante sem considerar juros futuros. Não preenchível.",
            "saldo_devedor_com_juros": "Saldo restante considerando juros futuros.",
            "tuplas": (
                "Estrutura detalhada das parcelas. "
                "Cada tupla contém:  parcela, data da parcela, empresa, banco, número da parcela, juros, amortização."
            )
        }

        self.clear_main()

        card = ttk.Frame(self.main_frame, padding=20, style="Card.TFrame")
        card.pack(fill="both", expand=True)

        ttk.Label(
            card,
            text="Bem-vindo à Gestão Financeira",
            font=self.font_header
        ).pack(anchor="w", pady=(0, 8))

        desc = (
            "Aplicação para gerenciar empréstimos por bancos (tabelas). "
            "Permite visualizar, editar, importar/exportar dados, "
            "gerar gráficos e manter histórico auditável das alterações."
        )

        ttk.Label(
            card,
            text=desc,
            wraplength=1100
        ).pack(anchor="w", pady=(4, 12))

        quick = ttk.Frame(card)
        quick.pack(anchor="w", pady=8)

        ttk.Button(quick, text="Ver Tabelas", command=self.show_tables).pack(side="left", padx=6)
        ttk.Button(quick, text="Gerar Gráficos", command=self.show_graphs).pack(side="left", padx=6)

        # ---------- ÁREA DE AJUDA ----------
        ttk.Separator(card).pack(fill="x", pady=12)

        ttk.Label(
            card,
            text="Descrição dos campos da tabela",
            font=("Segoe UI", 12, "bold")
        ).pack(anchor="w", pady=(0, 6))

        help_container = ttk.Frame(card)
        help_container.pack(fill="both", expand=True)

        canvas = tk.Canvas(help_container, highlightthickness=0)
        canvas.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(help_container, orient="vertical", command=canvas.yview)
        scrollbar.pack(side="right", fill="y")

        canvas.configure(yscrollcommand=scrollbar.set)

        inner = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        inner.bind("<Configure>", _on_configure)

        for campo, descricao in FIELD_DESCRIPTIONS.items():
            row = ttk.Frame(inner)
            row.pack(fill="x", pady=4)

            ttk.Label(
                row,
                text=campo,
                width=25,
                anchor="w",
                font=("Segoe UI", 10, "bold")
            ).pack(side="left")

            ttk.Label(
                row,
                text=descricao,
                wraplength=800,
                justify="left"
            ).pack(side="left", fill="x", expand=True)


    # robust scroll behavior to avoid overscroll
    def _make_canvas_scrollable(self, canvas, inner_frame):
        def on_frame_config(e=None):
            bbox = canvas.bbox("all")
            canvas.configure(scrollregion=bbox if bbox else (0,0,0,0))
            if not bbox:
                return
            content_height = bbox[3] - bbox[1]
            content_width = bbox[2] - bbox[0]
            try:
                ch = canvas.winfo_height()
                cw = canvas.winfo_width()
            except Exception:
                ch = cw = 0
            if content_height <= ch:
                canvas.yview_moveto(0)
            if content_width <= cw:
                canvas.xview_moveto(0)
        inner_frame.bind("<Configure>", lambda e: on_frame_config())

        def _on_mousewheel(event):
            bbox = canvas.bbox("all")
            if not bbox:
                return "break"
            content_height = bbox[3] - bbox[1]
            content_width = bbox[2] - bbox[0]
            ch = canvas.winfo_height()
            cw = canvas.winfo_width()
            if content_height > ch:
                delta = int(-1*(event.delta/120))
                canvas.yview_scroll(delta, "units")
            if event.state & 0x1:
                if content_width > cw:
                    delta = int(-1*(event.delta/120))
                    canvas.xview_scroll(delta, "units")
            return "break"

        def _enter(e):
            b = "<MouseWheel>"
            self.bind_all(b, _on_mousewheel)
            self._active_canvas_bindings.append(b)
        def _leave(e):
            b = "<MouseWheel>"
            try:
                self.unbind_all(b)
            except Exception:
                pass
            if b in self._active_canvas_bindings:
                self._active_canvas_bindings.remove(b)
        canvas.bind("<Enter>", _enter)
        canvas.bind("<Leave>", _leave)
        inner_frame.update_idletasks()
        on_frame_config()

    # ----------------- Tables view -----------------
    def show_tables(self):
        self.clear_main()
        outer = ttk.Frame(self.main_frame)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text="Tabelas do Banco", font=self.font_header).pack(anchor="w", pady=(0,8))

        tables = listar_tabelas()
        topframe = ttk.Frame(outer)
        topframe.pack(fill="x")
        ttk.Label(topframe, text="Selecione tabelas para visualizar:", font=self.font_normal).pack(side="left")

        self.check_vars = {}
        chkframe = ttk.Frame(outer)
        chkframe.pack(fill="x", pady=8)
        col = 0
        row = 0
        for t in tables:
            var = tk.BooleanVar(value=t in self.selected_tables)
            cb = ttk.Checkbutton(chkframe, text=t, variable=var, command=self.update_table_display)
            cb.grid(row=row, column=col, padx=6, pady=4, sticky="w")
            self.check_vars[t] = var
            col += 1
            if col >= 5:
                col = 0
                row += 1

        all_empresas = set()
        for t in tables:
            df = fetch_table(t)
            if "empresa" in df.columns:
                all_empresas.update([str(e) for e in df["empresa"].unique() if pd.notnull(e)])
        if all_empresas:
            empframe = ttk.LabelFrame(outer, text="Filtrar por empresa", padding=8)
            empframe.pack(fill="x", pady=(6,10))
            self.empresa_vars = {}
            for idx, emp in enumerate(sorted(all_empresas)):
                var = tk.BooleanVar(value=emp in self.selected_empresas or not self.selected_empresas)
                cb = ttk.Checkbutton(empframe, text=emp, variable=var, command=self.update_table_display)
                cb.grid(row=0, column=idx, padx=6, sticky="w")
                self.empresa_vars[emp] = var

        ctl = ttk.Frame(outer)
        ctl.pack(fill="x", pady=(6,10))
        ttk.Button(ctl, text="Criar Tabela Padrão", command=self.create_table_dialog).pack(side="left", padx=6)
        ttk.Button(ctl, text="Excluir Tabela", command=self.delete_table_dialog).pack(side="left", padx=6)

        # sheet area with robust scrolling
        self.sheet_container = ttk.Frame(outer)
        self.sheet_container.pack(fill="both", expand=True)
        self.sheet_canvas = tk.Canvas(self.sheet_container, background="#f3f6fb", highlightthickness=0)
        self.sheet_canvas.pack(side="left", fill="both", expand=True)
        vsb = ttk.Scrollbar(self.sheet_container, orient="vertical", command=self.sheet_canvas.yview)
        vsb.pack(side="right", fill="y")
        hsb = ttk.Scrollbar(self.main_frame, orient="horizontal", command=self.sheet_canvas.xview)
        hsb.pack(side="bottom", fill="x")
        self.sheet_canvas.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.display_tables_frame = ttk.Frame(self.sheet_canvas)
        self.sheet_canvas.create_window((0,0), window=self.display_tables_frame, anchor="nw")
        self._make_canvas_scrollable(self.sheet_canvas, self.display_tables_frame)

        self.update_table_display()

    def delete_table_dialog(self):
        tables = listar_tabelas()
        if not tables:
            messagebox.showinfo("Aviso", "Não há tabelas para excluir.")
            return
        top = tk.Toplevel(self)
        top.title("Excluir Tabela")
        top.transient(self)
        ttk.Label(top, text="Selecione a tabela a excluir:").grid(row=0, column=0, padx=8, pady=8)
        combo = ttk.Combobox(top, values=tables, state="readonly")
        combo.grid(row=0, column=1, padx=8, pady=8)
        combo.set(tables[0])
        def do_delete():
            t = combo.get()
            if not t:
                return
            if messagebox.askyesno("Confirmação", f"Tem certeza que deseja excluir a tabela '{t}'? Esta operação é irreversível."):
                try:
                    drop_table(t)
                    messagebox.showinfo("Pronto", f"Tabela '{t}' excluída.")
                    top.destroy()
                    self.show_tables()
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao excluir tabela: {e}")
        ttk.Button(top, text="Excluir",  command=do_delete).grid(row=1, column=0, columnspan=2, pady=10)

    def update_table_display(self):
        for widget in self.display_tables_frame.winfo_children():
            widget.destroy()
        self.selected_tables = [t for t, v in self.check_vars.items() if v.get()]
        self.selected_empresas = [e for e, v in self.empresa_vars.items() if v.get()] if self.empresa_vars else []
        if not self.selected_tables:
            ttk.Label(self.display_tables_frame, text="Selecione ao menos uma tabela acima para visualizar.", padding=12).pack()
            return

        def safe_sum(series):
            s = series.astype(str).str.replace(",", ".")
            nums = pd.to_numeric(s, errors="coerce")
            return nums.sum()

        all_dfs = []
        for table in self.selected_tables:
            df = fetch_table(table)
            if self.selected_empresas and "empresa" in df.columns:
                df = df[df["empresa"].astype(str).isin(self.selected_empresas)]
            df["__tabela"] = table
            all_dfs.append(df)
        if not all_dfs:
            ttk.Label(self.display_tables_frame, text="Sem dados para as seleções.", padding=12).pack()
            return
        concat_df = pd.concat(all_dfs, ignore_index=True)
        concat_df = concat_df.sort_values(["__tabela", "id"] if "id" in concat_df.columns else ["__tabela"])

        cols = []
        for t in self.selected_tables:
            tcols = config.get_visual(t, list(fetch_table(t).columns))
            for c in tcols:
                if c not in cols and c in concat_df.columns:
                    cols.append(c)
        if "__tabela" not in cols:
            cols = ["__tabela"] + cols

        # create SheetFrame (which contains selection logic)
        self.sheet = SheetFrame(self.display_tables_frame, concat_df, cols, self)
        self.sheet.pack(fill="both", expand=True, padx=8, pady=8)

        sums_frame = ttk.Frame(self.display_tables_frame, padding=8)
        sums_frame.pack(fill="x")
        infos = []
        if "id" in concat_df.columns:
            infos.append(f"Total IDs: {concat_df['id'].count()}")
        for field in ["valor_adquirido", "saldo_devedor", "saldo_devedor_com_juros"]:
            if field in concat_df.columns:
                infos.append(f"Soma {field}: {format_number(safe_sum(concat_df[field]))}")
        ttk.Label(sums_frame, text=" | ".join(infos), font=self.font_normal).pack(anchor="w")
        self.display_tables_frame.update_idletasks()
        self.sheet_canvas.configure(scrollregion=self.sheet_canvas.bbox("all") or (0,0,0,0))

    def create_table_dialog(self):
        nome = simpledialog.askstring("Nova Tabela", "Nome da nova tabela padrão:")
        if not nome:
            return
        try:
            criar_tabela_padrao(nome)
            messagebox.showinfo("Pronto", f"Tabela '{nome}' criada!")
            self.show_tables()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao criar tabela: {e}")

    # ----------------- Import / Export / DB selection / Config / Reports (kept) -----------------
    def show_import_dialog(self):
        file = filedialog.askopenfilename(title="Escolha o arquivo para importar", filetypes=[("Arquivos", "*.*")])
        if not file:
            return
        tables = listar_tabelas()
        if not tables:
            messagebox.showerror("Erro", "Nenhuma tabela disponível para importar.")
            return
        top = tk.Toplevel(self)
        top.title("Importar arquivo")
        top.transient(self)
        ttk.Label(top, text=f"Arquivo: {file}").grid(row=0, column=0, columnspan=2, padx=8, pady=8)
        ttk.Label(top, text="Tabela destino:").grid(row=1, column=0, padx=8, pady=6)
        combo = ttk.Combobox(top, values=tables, state="readonly")
        combo.grid(row=1, column=1, padx=8, pady=6)
        combo.set(tables[0])
        def do_import():
            tab = combo.get()
            res = import_file_to_table(file, tab)
            if res.get("ok"):
                messagebox.showinfo("Importação concluída", f"Insert: {res.get('inserted',0)}, Update: {res.get('updated',0)}")
                top.destroy()
                self.update_table_display()
            else:
                messagebox.showerror("Erro", res.get("message"))
        ttk.Button(top, text="Importar",  command=do_import).grid(row=2, column=0, columnspan=2, pady=10)

    def show_export_dialog(self):
        tables = listar_tabelas()
        if not tables:
            messagebox.showerror("Erro", "Nenhuma tabela disponível para exportar.")
            return
        top = tk.Toplevel(self)
        top.title("Exportar Dados")
        top.transient(self)
        ttk.Label(top, text="Formatos:").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        var_excel = tk.BooleanVar(value=True)
        var_csv = tk.BooleanVar(value=False)
        var_json = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Excel", variable=var_excel).grid(row=0, column=1, sticky="w")
        ttk.Checkbutton(top, text="CSV", variable=var_csv).grid(row=0, column=2, sticky="w")
        ttk.Checkbutton(top, text="JSON", variable=var_json).grid(row=0, column=3, sticky="w")
        ttk.Label(top, text="Tabelas:").grid(row=1, column=0, sticky="w", padx=8)
        tbl_vars = {}
        frame_tbl = ttk.Frame(top)
        frame_tbl.grid(row=2, column=0, columnspan=4, sticky="w", padx=8)
        for idx, t in enumerate(tables):
            v = tk.BooleanVar(value=False)
            cb = ttk.Checkbutton(frame_tbl, text=t, variable=v)
            cb.grid(row=idx//4, column=idx%4, sticky="w", padx=6, pady=4)
            tbl_vars[t] = v

        def do_export():
            formats = []
            if var_excel.get(): formats.append("excel")
            if var_csv.get(): formats.append("csv")
            if var_json.get(): formats.append("json")
            if not formats:
                messagebox.showerror("Erro", "Escolha ao menos um formato.")
                return
            selected = [t for t, v in tbl_vars.items() if v.get()]
            if not selected:
                selected = tables
            tsfolder = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            outpaths = []
            for t in selected:
                df = fetch_table(t)
                res = export_dataframe(df, formats, base_folder_name=tsfolder, filename_base=t)
                if res.get("ok"):
                    outpaths += res.get("paths", [])
            # ensure geral sheet included in mirror
            mirror_db_to_excel()
            if outpaths:
                messagebox.showinfo("Exportado", f"Arquivos gerados em: {os.path.join(EXPORT_DIR, tsfolder)}")
                top.destroy()
            else:
                messagebox.showerror("Erro", "Falha ao exportar.")
        ttk.Button(top, text="Exportar",  command=do_export).grid(row=3, column=0, columnspan=4, pady=10)

    def select_db_dialog(self):
        file = filedialog.askopenfilename(title="Selecione o arquivo DB", filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("Todos", "*.*")])
        if not file:
            return
        try:
            con = sqlite3.connect(file)
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            con.close()
            global DB_FILE
            DB_FILE = file
            config.set_db_path(file)
            messagebox.showinfo("Pronto", f"Banco de dados selecionado: {file}")
            self.show_tables()
        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível abrir o DB: {e}")

    def show_config(self):
        self.clear_main()
        tabs = ttk.Notebook(self.main_frame)
        tabs.pack(fill="both", expand=True, padx=12, pady=12)

        # Visualização tab
        visualtab = ttk.Frame(tabs, padding=8)
        tabs.add(visualtab, text="Visualização")
        ttk.Label(visualtab, text="Tabela:", font=self.font_normal).pack(anchor="w")
        tables = listar_tabelas()
        table_combo = ttk.Combobox(visualtab, values=["*"]+tables)
        table_combo.pack(fill="x", pady=6)
        cols_listbox = tk.Listbox(visualtab, selectmode="multiple", height=10)
        cols_listbox.pack(fill="both", expand=True)
        def on_sel_table(event=None):
            tab = table_combo.get()
            if tab == "":
                return
            allcols = get_table_columns(tables[0]) if tables else []
            if tab != "*":
                allcols = get_table_columns(tab)
            cols_listbox.delete(0, tk.END)
            for c in allcols:
                cols_listbox.insert(tk.END, c)
            selected = config.visual_cols.get(tab, allcols)
            for idx, c in enumerate(allcols):
                if c in selected:
                    cols_listbox.selection_set(idx)
        table_combo.bind("<<ComboboxSelected>>", on_sel_table)
        def save_visual():
            tab = table_combo.get()
            allcols = cols_listbox.get(0, tk.END)
            selected = [allcols[i] for i in cols_listbox.curselection()]
            if tab:
                config.set_visual(tab, selected)
            messagebox.showinfo("Pronto", "Configuração salva!")
        ttk.Button(visualtab, text="Salvar", command=save_visual).pack(pady=6)

        # Relatório tab
        reporttab = ttk.Frame(tabs, padding=8)
        tabs.add(reporttab, text="Relatório")
        ttk.Label(reporttab, text="Escolha colunas para relatório:").pack(anchor="w")
        allcols = get_table_columns(tables[0]) if tables else []
        report_cols_listbox = tk.Listbox(reporttab, selectmode="multiple", height=10)
        for c in allcols:
            report_cols_listbox.insert(tk.END, c)
        report_cols_listbox.pack(fill="both", expand=True)
        def save_report():
            sel = [allcols[i] for i in report_cols_listbox.curselection()]
            config.set_report(sel)
            messagebox.showinfo("Pronto", "Configuração salva!")
        ttk.Button(reporttab, text="Salvar", command=save_report).pack(pady=6)

        # Tipos de coluna tab
        typetab = ttk.Frame(tabs, padding=8)
        tabs.add(typetab, text="Tipos de Coluna")
        ttk.Label(typetab, text="Tabela:").grid(row=0, column=0, sticky="w")
        table_combo2 = ttk.Combobox(typetab, values=["*"]+tables)
        table_combo2.grid(row=0, column=1, sticky="w")
        cols_frame = ttk.Frame(typetab)
        cols_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=6)
        typemap_widgets = {}

        def on_table_select_types(event=None):
            for w in cols_frame.winfo_children():
                w.destroy()
            typemap_widgets.clear()
            tab = table_combo2.get()
            if not tab:
                return
            if tab == "*":
                allcols = []
                for t in listar_tabelas():
                    for c in get_table_columns(t):
                        if c not in allcols:
                            allcols.append(c)
            else:
                allcols = get_table_columns(tab)
            for idx, c in enumerate(allcols):
                ttk.Label(cols_frame, text=c+":").grid(row=idx, column=0, sticky="w", padx=4, pady=2)
                cb = ttk.Combobox(cols_frame, values=["text", "int", "float", "date"], width=12)
                cb.grid(row=idx, column=1, sticky="w", padx=4, pady=2)
                cb.set(config.get_col_type(tab, c))
                typemap_widgets[c] = cb

        table_combo2.bind("<<ComboboxSelected>>", on_table_select_types)

        def save_types():
            tab = table_combo2.get()
            if not tab:
                messagebox.showerror("Erro", "Selecione uma tabela (ou *)")
                return
            for c, widget in typemap_widgets.items():
                config.set_col_type(tab, c, widget.get())
            messagebox.showinfo("Pronto", "Tipos salvos.")

        ttk.Button(typetab, text="Salvar Tipos", command=save_types).grid(row=2, column=0, columnspan=2, pady=8)

        # Padronização de colunas tab (with required checkbox next to fixed/free)
        pad_tab = ttk.Frame(tabs, padding=8)
        tabs.add(pad_tab, text="Padronização de Colunas")
        ttk.Label(pad_tab, text="Tabela (ou * aplica a todas):").grid(row=0, column=0, sticky="w")
        pad_table_combo = ttk.Combobox(pad_tab, values=["*"]+tables)
        pad_table_combo.grid(row=0, column=1, sticky="w")
        pad_cols_frame = ttk.Frame(pad_tab)
        pad_cols_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=6)
        pad_widgets = {}

        def on_pad_table_selected(event=None):
            for w in pad_cols_frame.winfo_children():
                w.destroy()
            pad_widgets.clear()
            tab = pad_table_combo.get()
            if not tab:
                return
            if tab == "*":
                allcols = []
                for t in listar_tabelas():
                    for c in get_table_columns(t):
                        if c not in allcols:
                            allcols.append(c)
            else:
                allcols = get_table_columns(tab)
            for idx, c in enumerate(allcols):
                ttk.Label(pad_cols_frame, text=c+":").grid(row=idx, column=0, sticky="w", padx=4, pady=2)
                mode_cb = ttk.Combobox(pad_cols_frame, values=["free", "fixed"], width=8)
                mode_cb.grid(row=idx, column=1, sticky="w", padx=4, pady=2)
                std = config.get_col_standardization(tab, c)
                mode_cb.set(std.get("mode", "free"))
                vals_entry = ttk.Entry(pad_cols_frame, width=40)
                vals_entry.grid(row=idx, column=2, sticky="w", padx=4, pady=2)
                vals_entry.insert(0, ",".join(std.get("values", [])))
                required_var = tk.BooleanVar(value=std.get("required", False))
                req_cb = ttk.Checkbutton(pad_cols_frame, text="Obrigatório", variable=required_var)
                req_cb.grid(row=idx, column=3, sticky="w", padx=6, pady=2)
                pad_widgets[c] = (mode_cb, vals_entry, required_var)

        pad_table_combo.bind("<<ComboboxSelected>>", on_pad_table_selected)

        def save_padronizacao():
            tab = pad_table_combo.get()
            if not tab:
                messagebox.showerror("Erro", "Selecione uma tabela (ou *)")
                return
            # Save to selected key
            for c, (mode_widget, val_widget, req_var) in pad_widgets.items():
                mode = mode_widget.get()
                values = [v.strip() for v in val_widget.get().split(",") if v.strip()] if mode == "fixed" else []
                required = bool(req_var.get())
                config.set_col_standardization(tab, c, mode, values, required=required)
                # If saving for "*", propagate to tables that don't have explicit setting for this column.
                if tab == "*":
                    for t in listar_tabelas():
                        try:
                            cols = get_table_columns(t)
                        except Exception:
                            cols = []
                        if c in cols:
                            if not (t in config.col_standardization and c in config.col_standardization.get(t, {})):
                                config.set_col_standardization(t, c, mode, values, required=required)
            messagebox.showinfo("Pronto", "Padronização salva e propagada (quando aplicável).")
        ttk.Button(pad_tab, text="Salvar Padronização", command=save_padronizacao).grid(row=2, column=0, columnspan=2, pady=8)

        dbtab = ttk.Frame(tabs, padding=8)
        tabs.add(dbtab, text="Banco de Dados")
        ttk.Label(dbtab, text=f"DB atual: {DB_FILE}", wraplength=1000).pack(anchor="w", padx=8, pady=6)
        def clear_db_setting():
            global DB_FILE
            DB_FILE = DEFAULT_DB
            config.set_db_path("")
            messagebox.showinfo("Pronto", "Retornou ao DB padrão.")
            self.show_config()
        ttk.Button(dbtab, text="Resetar para DB padrão", command=clear_db_setting).pack(padx=8, pady=4)

    def show_reports(self):
        self.clear_main()
        ttk.Label(self.main_frame, text="Relatórios", font=self.font_header).pack(anchor="w", pady=(0,8))
        btn_frame = ttk.Frame(self.main_frame)
        btn_frame.pack(pady=6, anchor="w")
        ttk.Button(btn_frame, text="Por empresa", command=lambda: self.show_report_mode("empresa")).pack(side="left", padx=6)
        ttk.Button(btn_frame, text="Por banco", command=lambda: self.show_report_mode("banco")).pack(side="left", padx=6)
        ttk.Button(btn_frame, text="Geral", command=lambda: self.show_report_mode("geral")).pack(side="left", padx=6)
        self.report_area = tk.Text(self.main_frame, width=120, height=28, bg="#ffffff", relief="solid", bd=1)
        self.report_area.pack(fill="both", expand=False, padx=8, pady=8)

    def show_report_mode(self, mode):
        tables = listar_tabelas()
        allcols = get_table_columns(tables[0]) if tables else []
        default_cols = [c for c in allcols if c != "id"]
        selcols = config.get_report(default_cols)
        text = ""
        def safe_sum(series):
            s = series.astype(str).str.replace(",", ".")
            nums = pd.to_numeric(s, errors="coerce")
            return nums.sum()
        def safe_mean(series):
            s = series.astype(str).str.replace(",", ".")
            nums = pd.to_numeric(s, errors="coerce")
            return nums.mean()
        def is_num_col(series):
            try:
                nums = pd.to_numeric(series.astype(str).str.replace(",", "."), errors="coerce")
                return nums.notnull().sum() > 0
            except Exception:
                return False

        if mode == "empresa":
            dfs = []
            for t in tables:
                df = fetch_table(t)
                if len(df) > 0:
                    df["banco"] = t
                    dfs.append(df)
            if dfs:
                bigdf = pd.concat(dfs, ignore_index=True)
                if "empresa" in bigdf.columns:
                    for emp, dfg in bigdf.groupby("empresa"):
                        if str(emp).strip() == "" or dfg.empty:
                            continue
                        text += f"Empresa: {emp}\n"
                        for col in selcols:
                            if col == "id": continue
                            if col in dfg.columns and is_num_col(dfg[col]):
                                soma = safe_sum(dfg[col])
                                media = safe_mean(dfg[col])
                                text += f"  {col}: soma={format_number(soma)}, média={format_number(media)}\n"
                        text += "\n"
                self._last_report_df = bigdf
            else:
                self._last_report_df = pd.DataFrame()
        elif mode == "banco":
            text = ""
            biglist = []
            for t in tables:
                df = fetch_table(t)
                if len(df) == 0:
                    continue
                text += f"Banco: {t}\n"
                for col in selcols:
                    if col == "id": continue
                    if col in df.columns and is_num_col(df[col]):
                        soma = safe_sum(df[col])
                        media = safe_mean(df[col])
                        text += f"  {col}: soma={format_number(soma)}, média={format_number(media)}\n"
                text += "\n"
                df["__banco"] = t
                biglist.append(df)
            self._last_report_df = pd.concat(biglist, ignore_index=True) if biglist else pd.DataFrame()
        elif mode == "geral":
            dfs = []
            for t in tables:
                df = fetch_table(t)
                dfs.append(df)
            if dfs:
                bigdf = pd.concat(dfs, ignore_index=True)
                text += "Geral:\n"
                for col in selcols:
                    if col == "id": continue
                    if col in bigdf.columns and is_num_col(bigdf[col]):
                        soma = safe_sum(bigdf[col])
                        media = safe_mean(bigdf[col])
                        text += f"  {col}: soma={format_number(soma)}, média={format_number(media)}\n"
                self._last_report_df = bigdf
            else:
                self._last_report_df = pd.DataFrame()

        self.report_area.delete(1.0, tk.END)
        self.report_area.insert(tk.END, text)

    # ----------------- Graphs -----------------
    def show_graphs(self):
        self.clear_main()
        ttk.Label(self.main_frame, text="Gerador de Gráficos (coluna 'tuplas')", font=self.font_header).pack(anchor="w", pady=(0,8))

        frame = ttk.Frame(self.main_frame)
        frame.pack(fill="x", pady=6)

        banks = listar_tabelas()
        ttk.Label(frame, text="Bancos (tabelas):").grid(row=0, column=0, sticky="w", padx=6)
        self.graph_bank_vars = {}
        bank_frame = ttk.Frame(frame)
        bank_frame.grid(row=1, column=0, sticky="w", padx=6, pady=4)
        for idx, b in enumerate(banks):
            v = tk.BooleanVar(value=False)
            cb = ttk.Checkbutton(bank_frame, text=b, variable=v, command=self._update_graph_companies_and_range)
            cb.grid(row=idx//4, column=idx%4, padx=6, pady=2, sticky="w")
            self.graph_bank_vars[b] = v

        ttk.Label(frame, text="Empresas (filtrar):").grid(row=0, column=1, sticky="w", padx=6)
        self.graph_company_vars = {}
        self.company_frame = ttk.Frame(frame)
        self.company_frame.grid(row=1, column=1, sticky="w", padx=6, pady=4)

        ttk.Label(frame, text="Métrica:").grid(row=2, column=0, sticky="w", padx=6, pady=(8,0))
        self.metric_var = tk.StringVar(value="parcelas")
        metrics = [("Soma das parcelas (valor pago)", "parcelas"),
                   ("Soma da amortização", "amortizacao"),
                   ("Soma dos juros", "juros")]
        for i, (label, val) in enumerate(metrics):
            ttk.Radiobutton(frame, text=label, variable=self.metric_var, value=val).grid(row=3+i, column=0, columnspan=2, sticky="w", padx=12)

        ttk.Label(frame, text="Agregação:").grid(row=2, column=2, sticky="w", padx=6)
        self.grouping_var = tk.StringVar(value="por_empresa")
        ttk.Radiobutton(frame, text="Por empresa (linha por empresa)", variable=self.grouping_var, value="por_empresa").grid(row=3, column=2, sticky="w", padx=6)
        ttk.Radiobutton(frame, text="Por banco (linha por banco)", variable=self.grouping_var, value="por_banco").grid(row=4, column=2, sticky="w", padx=6)
        ttk.Radiobutton(frame, text="Tudo (uma linha única agregada)", variable=self.grouping_var, value="tudo").grid(row=5, column=2, sticky="w", padx=6)

        ttk.Label(frame, text="Período (início):").grid(row=2, column=3, sticky="w", padx=6)
        ttk.Label(frame, text="Período (fim):").grid(row=4, column=3, sticky="w", padx=6)
        self.start_month_cb = ttk.Combobox(frame, values=[], width=10, state="readonly")
        self.start_year_cb = ttk.Combobox(frame, values=[], width=7, state="readonly")
        self.end_month_cb = ttk.Combobox(frame, values=[], width=10, state="readonly")
        self.end_year_cb = ttk.Combobox(frame, values=[], width=7, state="readonly")
        self.start_month_cb.grid(row=3, column=3, padx=6, sticky="w")
        self.start_year_cb.grid(row=3, column=4, padx=6, sticky="w")
        self.end_month_cb.grid(row=5, column=3, padx=6, sticky="w")
        self.end_year_cb.grid(row=5, column=4, padx=6, sticky="w")

        btn_frame = ttk.Frame(self.main_frame)
        btn_frame.pack(fill="x", pady=8)
        ttk.Button(btn_frame, text="Atualizar empresas / periodo (após selecionar bancos)", command=self._update_graph_companies_and_range).pack(side="left", padx=8)
        ttk.Button(btn_frame, text="Gerar Gráfico", command=self._generate_graph).pack(side="left", padx=8)

        self.graph_canvas_holder = ttk.Frame(self.main_frame)
        self.graph_canvas_holder.pack(fill="both", expand=True, padx=8, pady=8)

    def _parse_tuplas_field(self, val):
        if val is None:
            return []
        if isinstance(val, list):
            return val
        s = str(val).strip()
        if s == "" or s.lower() == "nan":
            return []
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
        except Exception:
            try:
                s2 = s.replace("“", '"').replace("”", '"').replace("'", '"')
                parsed = ast.literal_eval(s2)
                if isinstance(parsed, (list, tuple)):
                    return list(parsed)
            except Exception:
                return []
        return []

    def _update_graph_companies_and_range(self):
        selected_banks = [b for b, v in self.graph_bank_vars.items() if v.get()]
        for w in self.company_frame.winfo_children():
            w.destroy()
        self.graph_company_vars = {}
        all_companies = set()
        all_dates = []
        for b in selected_banks:
            df = fetch_table(b)
            if "tuplas" not in df.columns:
                continue
            for _, row in df.iterrows():
                tupl = self._parse_tuplas_field(row.get("tuplas"))
                for t in tupl:
                    try:
                        empresa = str(t[2]) if len(t) > 2 else ""
                        date_str = t[1] if len(t) > 1 else None
                        all_companies.add(empresa)
                        if date_str:
                            dt = datetime.strptime(date_str, "%d-%m-%Y")
                            all_dates.append(dt)
                    except Exception:
                        continue
        sorted_companies = sorted([c for c in all_companies if c.strip() != ""])
        for idx, comp in enumerate(sorted_companies):
            v = tk.BooleanVar(value=True)
            cb = ttk.Checkbutton(self.company_frame, text=comp, variable=v)
            cb.grid(row=idx//4, column=idx%4, padx=6, pady=2, sticky="w")
            self.graph_company_vars[comp] = v
        months = [f"{m:02d}" for m in range(1, 13)]
        if all_dates:
            min_dt = min(all_dates)
            max_dt = max(all_dates)
            years = list(range(min_dt.year, max_dt.year+1))
            self.start_month_cb['values'] = months
            self.end_month_cb['values'] = months
            self.start_year_cb['values'] = [str(y) for y in years]
            self.end_year_cb['values'] = [str(y) for y in years]
            self.start_month_cb.set(f"{min_dt.month:02d}")
            self.start_year_cb.set(str(min_dt.year))
            self.end_month_cb.set(f"{max_dt.month:02d}")
            self.end_year_cb.set(str(max_dt.year))
        else:
            now = datetime.now()
            years = list(range(now.year-5, now.year+6))
            self.start_month_cb['values'] = months
            self.end_month_cb['values'] = months
            self.start_year_cb['values'] = [str(y) for y in years]
            self.end_year_cb['values'] = [str(y) for y in years]
            self.start_month_cb.set(f"{now.month:02d}")
            self.start_year_cb.set(str(now.year))
            self.end_month_cb.set(f"{now.month:02d}")
            self.end_year_cb.set(str(now.year))

    def _generate_graph(self):
        selected_banks = [b for b, v in self.graph_bank_vars.items() if v.get()]
        if not selected_banks:
            messagebox.showerror("Erro", "Selecione ao menos um banco (tabela).")
            return
        selected_companies = [c for c, v in self.graph_company_vars.items() if v.get()]
        if not selected_companies:
            messagebox.showerror("Erro", "Selecione ao menos uma empresa.")
            return
        metric = self.metric_var.get()
        grouping = self.grouping_var.get()
        try:
            sm = int(self.start_month_cb.get())
            sy = int(self.start_year_cb.get())
            em = int(self.end_month_cb.get())
            ey = int(self.end_year_cb.get())
            start_dt = datetime(sy, sm, 1)
            if em == 12:
                end_dt = datetime(ey+1, 1, 1)
            else:
                end_dt = datetime(ey, em+1, 1)
        except Exception:
            messagebox.showerror("Erro", "Período inválido. Selecione mês e ano.")
            return

        agg = {}
        groups = set()
        for b in selected_banks:
            df = fetch_table(b)
            if "tuplas" not in df.columns:
                continue
            for _, row in df.iterrows():
                tupl = self._parse_tuplas_field(row.get("tuplas"))
                for t in tupl:
                    try:
                        valor_pago = float(t[0]) if t[0] is not None else 0.0
                        date_str = t[1]
                        empresa = str(t[2])
                        amortizacao = float(t[5]) if len(t) > 5 and t[5] is not None else 0.0
                        juros = valor_pago - amortizacao
                        dt = datetime.strptime(date_str, "%d-%m-%Y")
                    except Exception:
                        continue
                    if not (start_dt <= dt < end_dt):
                        continue
                    if empresa not in selected_companies:
                        continue
                    if grouping == "por_empresa":
                        group = empresa
                    elif grouping == "por_banco":
                        group = b
                    else:
                        group = "TOTAL"
                    groups.add(group)
                    key = (dt.year, dt.month)
                    if key not in agg:
                        agg[key] = {}
                    if group not in agg[key]:
                        agg[key][group] = 0.0
                    if metric == "parcelas":
                        agg[key][group] += valor_pago
                    elif metric == "amortizacao":
                        agg[key][group] += amortizacao
                    elif metric == "juros":
                        agg[key][group] += juros

        if not agg:
            messagebox.showinfo("Aviso", "Nenhum dado encontrado para os filtros selecionados.")
            return

        keys_sorted = sorted(agg.keys())
        index = [datetime(y, m, 1) for (y, m) in keys_sorted]
        data = {g: [] for g in sorted(groups)}
        for k in keys_sorted:
            row = agg.get(k, {})
            for g in sorted(groups):
                data[g].append(row.get(g, 0.0))
        plot_df = pd.DataFrame(data, index=index)

        for w in self.graph_canvas_holder.winfo_children():
            w.destroy()
        fig, ax = plt.subplots(figsize=(11, 5.5))
        lines = []
        for col in plot_df.columns:
            line, = ax.plot(plot_df.index, plot_df[col], marker='o', label=col)
            lines.append(line)
        ax.set_title({
            "parcelas": "Soma das parcelas por mês",
            "amortizacao": "Soma da amortização por mês",
            "juros": "Soma dos juros por mês"
        }.get(metric, "Gráfico"))
        ax.set_xlabel("Data (mês)")
        ax.set_ylabel("Valor (R$)")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend()
        fig.autofmt_xdate()

        canvas = FigureCanvasTkAgg(fig, master=self.graph_canvas_holder)
        widget = canvas.get_tk_widget()
        widget.pack(fill="both", expand=True)
        toolbar = NavigationToolbar2Tk(canvas, self.graph_canvas_holder)
        toolbar.update()
        toolbar.pack()

        if MPLCURSORS_AVAILABLE:
            try:
                cursor = mplcursors.cursor(lines, hover=True)
                @cursor.connect("add")
                def on_add(sel):
                    ix = int(round(sel.index)) if hasattr(sel, "index") else None
                    y = sel.target[1]
                    if ix is not None and 0 <= ix < len(plot_df.index):
                        dt = plot_df.index[ix]
                        sel.annotation.set_text(f"{sel.artist.get_label()}\n{dt.strftime('%b %Y')}\nR$ {y:,.2f}")
                    else:
                        sel.annotation.set_text(f"{sel.artist.get_label()}\nR$ {y:,.2f}")
            except Exception:
                pass

        self._graph_canvas = canvas
        self._graph_toolbar = toolbar
        canvas.draw()

    # ----------------- SheetFrame selection, move row, validation handled below -----------------

    # ----------------- NEW: run external update script -----------------
    def run_update_script(self):
        """
        Runs gerenciador_de_contratos_v3.py (must be in same directory) in a background thread.
        Disables the Atualizar button while running, shows results when finished, and refreshes views.
        """
        script_path = os.path.join(BASE_DIR, "gerenciador_de_contratros_v3.py")
        if not os.path.isfile(script_path):
            messagebox.showerror("Erro", f"Script não encontrado: {script_path}")
            return

        # disable button during run
        if self._update_button:
            self._update_button.config(state="disabled", text="Atualizando...")

        def _run():
            try:
                # Run with same interpreter
                proc = subprocess.run([sys.executable, script_path], capture_output=True, text=True, cwd=BASE_DIR, timeout=600)
                stdout = proc.stdout or ""
                stderr = proc.stderr or ""
                returncode = proc.returncode
            except subprocess.TimeoutExpired as e:
                stdout = e.stdout or ""
                stderr = f"TimeoutExpired: {e}"
                returncode = -1
            except Exception as e:
                stdout = ""
                stderr = traceback.format_exc()
                returncode = -2

            # write outputs to log file
            try:
                ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                logpath = os.path.join(LOGS_DIR, f"atualizar_output_{ts}.log")
                with open(logpath, "w", encoding="utf-8") as f:
                    f.write("=== STDOUT ===\n")
                    f.write(stdout + "\n")
                    f.write("=== STDERR ===\n")
                    f.write(stderr + "\n")
                logmsg = f"Saída gravada em {logpath}"
            except Exception:
                logmsg = "Falha ao gravar log de saída."

            # schedule UI update on main thread
            def _on_complete():
                if self._update_button:
                    self._update_button.config(state="normal", text="Atualizar")
                if returncode == 0:
                    messagebox.showinfo("Atualização concluída", f"Script executado com sucesso.\n{logmsg}")
                else:
                    messagebox.showerror("Atualização com erro", f"Código de saída: {returncode}\nVerifique o log: {logmsg}")
                # After update, refresh mirror and current view
                try:
                    mirror_db_to_excel()
                except Exception:
                    pass
                try:
                    # If currently viewing tables, refresh
                    self.show_tables()
                except Exception:
                    pass

            self.after(100, _on_complete)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

# ----------------- SheetFrame Implementation -----------------
class SheetFrame(ttk.Frame):
    def __init__(self, master, df, columns, gui):
        super().__init__(master, padding=6)
        self.df = df.copy().reset_index(drop=True)
        self.columns = columns
        self.gui = gui
        self.sort_state = {}
        self.entries = {}
        self.selected_row = None
        self.sheet_frame = ttk.Frame(self)
        self.sheet_frame.pack(fill="both", expand=True)
        self.build_table()
        stats_row = len(self.df) + 1
        stats = []
        id_count = self.df['id'].count() if 'id' in self.df.columns else 0
        if 'id' in self.df.columns:
            stats.append(f"Total IDs: {id_count}")
        def safe_sum(field):
            if field not in self.df.columns:
                return 0
            s = self.df[field].astype(str).str.replace(",", ".")
            nums = pd.to_numeric(s, errors="coerce")
            return nums.sum()
        for field in ["valor_adquirido", "saldo_devedor", "saldo_devedor_com_juros"]:
            if field in self.df.columns:
                soma = safe_sum(field)
                stats.append(f"Soma {field}: {format_number(soma)}")
        ttk.Label(self.sheet_frame, text=" | ".join(stats)).grid(row=stats_row, column=0, columnspan=len(self.columns), sticky="ew", pady=(6,2))
        addbtn = ttk.Button(self.sheet_frame, text="+ Adicionar linha", command=self.add_row)
        addbtn.grid(row=stats_row+1, column=0, columnspan=max(1, len(self.columns)//3), sticky="ew", pady=(8,4))
        delbtn = ttk.Button(self.sheet_frame, text="Excluir linha selecionada", command=self.delete_selection)
        delbtn.grid(row=stats_row+1, column=max(1, len(self.columns)//3), columnspan=max(1, len(self.columns)//3), sticky="ew", pady=(8,4))
        movebtn = ttk.Button(self.sheet_frame, text="Mover linha para outra tabela", command=self.move_row_dialog)
        movebtn.grid(row=stats_row+1, column=2*max(1, len(self.columns)//3), columnspan=max(1, len(self.columns)//3), sticky="ew", pady=(8,4))

    def build_table(self):
        for w in self.sheet_frame.winfo_children():
            w.destroy()
        self.entries.clear()
        self.col_widths = []
        for j, col in enumerate(self.columns):
            header = ttk.Label(self.sheet_frame, text=col, background="#e6f2ff", anchor="center", padding=6)
            header.grid(row=0, column=j, sticky="nsew", padx=1, pady=1)
            header.bind("<Button-1>", lambda e, c=col: self.header_clicked(c))
            vals = [str(x) for x in self.df[col]] if col in self.df.columns else [""]
            width = max(12, min(36, max([len(str(col))]+[len(str(x)) for x in vals])))
            header.config(width=width)
            self.col_widths.append(width)
        for i, (_, row) in enumerate(self.df.iterrows(), start=1):
            for j, col in enumerate(self.columns):
                val = row.get(col, "")
                lbl = tk.Label(self.sheet_frame, text=str(val), bg="white", borderwidth=1, relief="solid", width=self.col_widths[j], anchor="w", padx=4)
                lbl.grid(row=i, column=j, sticky="nsew", padx=1, pady=1)
                # single-click select
                lbl.bind("<Button-1>", lambda e, ii=i, jj=j: self._select_row(ii, jj))
                # double-click edit (if editable)
                if col != "__tabela":
                    lbl.bind("<Double-1>", lambda e, rid=row.get("id", None), t=row.get("__tabela", None), c=col: self.cell_edit(e, rid, t, c))
                self.entries[(i, j)] = lbl

    def _select_row(self, i, j):
        self.selected_row = (i, j)
        for l in self.entries.values():
            l.config(bg="white")
        for jj in range(len(self.columns)):
            widget = self.entries.get((i, jj))
            if widget:
                widget.config(bg="#eef9ff")

    def header_clicked(self, col):
        asc = self.sort_state.get(col, True)
        try:
            if col not in self.df.columns:
                return
            try:
                tmp = pd.to_numeric(self.df[col].astype(str).str.replace(",", "."), errors="coerce")
                if tmp.notnull().sum() > 0:
                    self.df["_tmp_sort"] = tmp
                    self.df = self.df.sort_values("_tmp_sort", ascending=asc).drop(columns=["_tmp_sort"])
                else:
                    self.df = self.df.sort_values(col, ascending=asc)
            except Exception:
                self.df = self.df.sort_values(col, ascending=asc)
            self.sort_state[col] = not asc
            self.build_table()
        except Exception as e:
            print("Erro ao ordenar:", e)

    def validate_value(self, tablename, col, value, is_initial=False):
        t = config.get_col_type(tablename, col)
        if t == "text":
            pass
        elif t == "int":
            if value != "":
                try:
                    int(value)
                except Exception:
                    return False, "Valor deve ser inteiro", None
        elif t == "float":
            if value != "":
                if isinstance(value, str) and "," in value:
                    return False, "Para floats use ponto (.) como separador decimal", None
                try:
                    float(value)
                except Exception:
                    return False, "Valor deve ser decimal (ex: 1234.56)", None
        elif t == "date":
            if value != "":
                try:
                    datetime.strptime(value, "%d-%m-%Y")
                except Exception:
                    return False, "Formato de data inválido (dd-mm-aaaa)", None
        std = config.get_col_standardization(tablename, col)
        if std.get("mode") == "fixed":
            allowed = std.get("values", [])
            if value == "":
                if is_initial:
                    return True, "", value
                else:
                    return False, f"Esta coluna só aceita valores fixos. Escolha entre: {', '.join(allowed)}", None
            if value not in allowed:
                return False, f"Valor não permitido. Escolha entre: {', '.join(allowed)}", None
        return True, "", value

    def cell_edit(self, event, rowid, tablename, col):
        oldval = event.widget.cget("text")
        std = config.get_col_standardization(tablename, col)
        top = tk.Toplevel(self)
        top.title(f"Editar {col}")
        top.transient(self)
        ttk.Label(top, text=f"Novo valor para {col}:").pack(anchor="w", padx=8, pady=(8,4))
        if std.get("mode") == "fixed":
            values = std.get("values", [])
            combo = ttk.Combobox(top, values=values, width=60)
            combo.pack(padx=8, pady=6)
            combo.set(oldval if oldval in values else (values[0] if values else ""))
            combo.focus()
            def save_combo(event=None):
                newval = combo.get().strip()
                ok, msg, norm = self.validate_value(tablename, col, newval, is_initial=False)
                if not ok:
                    messagebox.showerror("Erro", msg)
                    return
                if rowid is None or pd.isna(rowid):
                    messagebox.showerror("Erro", "Linha não possui ID válido.")
                    return
                try:
                    update_cell(tablename, col, norm, rowid)
                    self.gui.update_table_display()
                    top.destroy()
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao salvar: {e}")
            combo.bind("<Return>", save_combo)
            ttk.Button(top, text="Salvar", command=save_combo).pack(pady=8)
        else:
            entry = ttk.Entry(top, width=60)
            entry.insert(0, oldval)
            entry.pack(padx=8, pady=6)
            entry.focus()
            def save_entry(event=None):
                newval = entry.get().strip()
                ok, msg, norm = self.validate_value(tablename, col, newval, is_initial=False)
                if not ok:
                    messagebox.showerror("Erro", msg)
                    return
                if rowid is None or pd.isna(rowid):
                    messagebox.showerror("Erro", "Linha não possui ID válido.")
                    return
                try:
                    update_cell(tablename, col, norm, rowid)
                    self.gui.update_table_display()
                    top.destroy()
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao salvar: {e}")
            entry.bind("<Return>", save_entry)
            ttk.Button(top, text="Salvar", command=save_entry).pack(pady=8)
        ttk.Button(top, text="Cancelar", command=top.destroy).pack(pady=(0,8))

    def add_row(self):
        def save():
            table = combo.get()
            if not table:
                messagebox.showerror("Erro", "Escolha a tabela!")
                return
            columns = get_table_columns(table)
            values = {}
            for c in columns:
                widget = entries[c]
                if isinstance(widget, ttk.Combobox):
                    values[c] = widget.get().strip()
                else:
                    values[c] = widget.get().strip()
            # required checks
            missing_required = []
            for c in columns:
                std = config.get_col_standardization(table, c)
                if std.get("required", False) and (values.get(c, "") is None or str(values.get(c, "")).strip() == ""):
                    missing_required.append(c)
            if missing_required:
                messagebox.showerror("Erro", f"As colunas obrigatórias não foram preenchidas: {', '.join(missing_required)}")
                return
            if not values.get("id"):
                messagebox.showerror("Erro", "O campo 'id' da linha não pode ficar vazio!")
                return
            if table_has_id(table, values["id"]):
                messagebox.showerror("Erro", f"Já existe um registro com id={values['id']} na tabela {table}.")
                return
            for c in columns:
                ok, msg, norm = self.validate_value(table, c, values[c], is_initial=True)
                if not ok:
                    messagebox.showerror("Erro", f"Coluna {c}: {msg}")
                    return
                values[c] = norm if norm is not None else ""
            try:
                insert_row(table, values)
                self.gui.update_table_display()
                top.destroy()
            except ValueError as ve:
                messagebox.showerror("Erro", str(ve))
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao inserir: {e}")

        top = tk.Toplevel(self)
        top.title("Adicionar Linha")
        top.transient(self)
        ttk.Label(top, text="Tabela:").grid(row=0, column=0, padx=8, pady=8)
        combo = ttk.Combobox(top, values=self.gui.selected_tables if self.gui.selected_tables else listar_tabelas(), state="readonly")
        combo.grid(row=0, column=1, padx=8, pady=8)
        if combo['values']:
            combo.set(combo['values'][0])
        entries = {}
        def on_table_change(evt=None):
            for w in top.grid_slaves():
                info = w.grid_info()
                r = info['row']
                if r >= 1:
                    w.destroy()
            nonlocal entries
            entries = {}
            cols = get_table_columns(combo.get())
            for idx, c in enumerate(cols):
                ttk.Label(top, text=c+":").grid(row=idx+1, column=0, padx=8, pady=4, sticky="w")
                std = config.get_col_standardization(combo.get(), c)
                if std.get("mode") == "fixed":
                    cb = ttk.Combobox(top, values=std.get("values", []), width=46)
                    cb.grid(row=idx+1, column=1, padx=8, pady=4, sticky="w")
                    entries[c] = cb
                else:
                    ent = ttk.Entry(top, width=48)
                    ent.grid(row=idx+1, column=1, padx=8, pady=4, sticky="w")
                    entries[c] = ent
                if std.get("required", False):
                    ttk.Label(top, text="*", foreground="red").grid(row=idx+1, column=2, sticky="w")
        combo.bind("<<ComboboxSelected>>", on_table_change)
        on_table_change()
        ttk.Button(top, text="Salvar", command=save).grid(row=999, column=0, columnspan=2, pady=12)

    def delete_selection(self):
        if not self.selected_row:
            messagebox.showerror("Erro", "Selecione uma linha para excluir.")
            return
        i, _ = self.selected_row
        row_idx = i-1
        if row_idx < 0 or row_idx >= len(self.df):
            messagebox.showerror("Erro", "Seleção inválida.")
            return
        row = self.df.iloc[row_idx]
        rowid = row.get("id", None)
        tablename = row.get("__tabela", None)
        if rowid is None or pd.isna(rowid) or not tablename:
            messagebox.showerror("Erro", "Linha não possui ID/tabela válida.")
            return
        if messagebox.askyesno("Confirmação", "Deseja excluir esta linha?"):
            try:
                delete_row(tablename, rowid)
                self.gui.update_table_display()
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao excluir: {e}")

    def move_row_dialog(self):
        if not self.selected_row:
            messagebox.showerror("Erro", "Selecione uma linha para mover.")
            return
        i, _ = self.selected_row
        row_idx = i-1
        if row_idx < 0 or row_idx >= len(self.df):
            messagebox.showerror("Erro", "Seleção inválida.")
            return
        row = self.df.iloc[row_idx]
        src_table = row.get("__tabela", None)
        rowid = row.get("id", None)
        if not src_table or rowid is None or pd.isna(rowid):
            messagebox.showerror("Erro", "Linha não possui ID/tabela válida.")
            return
        tables = listar_tabelas()
        dest_tables = [t for t in tables if t != src_table]
        if not dest_tables:
            messagebox.showerror("Erro", "Não há outra tabela destino para mover.")
            return
        top = tk.Toplevel(self)
        top.title("Mover linha para outra tabela")
        top.transient(self)
        ttk.Label(top, text=f"Origem: {src_table}   ID: {rowid}").grid(row=0, column=0, columnspan=2, padx=8, pady=6)
        ttk.Label(top, text="Destino:").grid(row=1, column=0, padx=8, pady=6)
        combo = ttk.Combobox(top, values=dest_tables, state="readonly")
        combo.grid(row=1, column=1, padx=8, pady=6)
        combo.set(dest_tables[0])
        ttk.Label(top, text="Se ID já existir no destino, informe novo ID:").grid(row=2, column=0, padx=8, pady=6)
        newid_entry = ttk.Entry(top, width=40)
        newid_entry.grid(row=2, column=1, padx=8, pady=6)
        ttk.Label(top, text="(Deixe em branco para manter o mesmo ID se possível)").grid(row=3, column=0, columnspan=2, padx=8, pady=4)

        def ask_fill_missing(dest, values, missing_cols):
            dlg = tk.Toplevel(self)
            dlg.title("Preencher campos obrigatórios")
            dlg.transient(self)
            entries_local = {}
            ttk.Label(dlg, text=f"Preencha os campos obrigatórios para mover para {dest}:").pack(anchor="w", padx=8, pady=6)
            form = ttk.Frame(dlg)
            form.pack(fill="both", expand=True, padx=8, pady=6)
            for idx, c in enumerate(missing_cols):
                ttk.Label(form, text=c + ":").grid(row=idx, column=0, sticky="w", padx=4, pady=4)
                ent = ttk.Entry(form, width=40)
                ent.grid(row=idx, column=1, padx=4, pady=4)
                entries_local[c] = ent
            result = {"ok": False, "values": values}
            def do_ok():
                for c, ent in entries_local.items():
                    v = ent.get().strip()
                    if v == "":
                        messagebox.showerror("Erro", f"O campo {c} é obrigatório.")
                        return
                    result["values"][c] = v
                result["ok"] = True
                dlg.destroy()
            def do_cancel():
                dlg.destroy()
            ttk.Button(dlg, text="OK", command=do_ok).pack(side="left", padx=8, pady=8)
            ttk.Button(dlg, text="Cancelar", command=do_cancel).pack(side="left", padx=8, pady=8)
            self.wait_window(dlg)
            return result if result["ok"] else None

        def do_move():
            dest = combo.get()
            if not dest:
                return
            new_id = newid_entry.get().strip()
            if new_id == "":
                new_id = rowid
            if table_has_id(dest, new_id):
                messagebox.showerror("Erro", f"ID {new_id} já existe na tabela {dest}. Escolha outro ID.")
                return
            dest_cols = get_table_columns(dest)
            src_vals = row.to_dict()
            values = {}
            for c in dest_cols:
                if c in src_vals:
                    v = src_vals[c]
                    values[c] = v if not pd.isna(v) else ""
                else:
                    values[c] = ""
            values["id"] = new_id
            missing_required = []
            for c in dest_cols:
                std = config.get_col_standardization(dest, c)
                if std.get("required", False) and (values.get(c, "") is None or str(values.get(c, "")).strip() == ""):
                    missing_required.append(c)
            if missing_required:
                res = ask_fill_missing(dest, values, missing_required)
                if res is None:
                    messagebox.showinfo("Cancelado", "Movimentação cancelada.")
                    return
                values = res["values"]
            try:
                insert_row(dest, values)
                delete_row(src_table, rowid)
                messagebox.showinfo("Pronto", f"Linha movida para {dest} com ID {new_id}")
                top.destroy()
                self.gui.update_table_display()
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao mover: {e}")

        ttk.Button(top, text="Mover",  command=do_move).grid(row=4, column=0, columnspan=2, pady=10)

# ----------------- Utilities -----------------
def format_number(x):
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

# ----------------- Start app -----------------
if __name__ == "__main__":
    from tkinter import messagebox
    import traceback

    try:
        copy_db_backup()
        mirror_db_to_excel_only_geral()
    except Exception as e:
        messagebox.showerror(
            "Erro ao iniciar",
            f"Ocorreu um erro ao preparar os dados:\n\n{e}"
        )
    try:
        con = sqlite3.connect(DB_FILE)
        con.close()
    except Exception:
        DB_FILE = DEFAULT_DB
    try:
        pass
        #mirror_db_to_excel()
    except Exception:
        pass
    app = FinanceManagerGUI()
    app.mainloop()
