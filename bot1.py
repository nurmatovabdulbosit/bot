from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
import sqlite3
import threading
import time as ttime
from datetime import datetime, time, timedelta
import pandas as pd
import os
import pytz
import asyncio

from config import BOT_TOKEN
from users import USERS, PROBLEM_REPORT_USERS
from sheets import get_dataframe

# =========================
# KONSTANTALAR
# =========================

CACHE_TTL = 60          # sekund
PAGE_SIZE = 5           # loyiha soni (pagination)
MAX_TEXT = 3800         # Telegram limit
DB_FILE = "projects.db"

# Ustun indekslari
COLUMN_INDEXES = {
    'project_name': 1,        # B - Loyiha nomi
    'korxona': 2,             # C - Korxona turi
    'loyiha_turi': 3,         # D - Loyiha turi
    'tuman': 5,               # F - Tuman
    'zona': 6,                # G - Zona
    'total_value': 13,        # N - Jami qiymat
    'yearly_value': 16,       # Q - 2026 yil uchun
    'size_type': 14,          # O - Loyiha hajmi
    'hamkor': 11,             # L - Hamkor
    'hamkor_mamlakat': 12,    # M - Hamkor mamlakati
    'holat': 27,              # AB - Loyiha holati
    'muammo': 28,             # AC - Muammo
    'boshqarma_masul': 29,    # AD - Boshqarmadan masul
    'viloyat_masul': 30,      # AE - Viloyat tashkilotdan masul
    'muammo_muddati': 32      # AG - Muammo muddati
}

# =========================
# YORDAMCHI FUNKSIYALAR
# =========================

def fmt(x):
    """Formatlash funksiyasi"""
    try:
        if x is None:
            return "0"
        return f"{round(float(x)):,}".replace(",", " ")
    except:
        return "0"

def safe_text(lines):
    """Telegram limitini hisobga olgan holda matn kesish"""
    text = ""
    for l in lines:
        if len(text) + len(l) > MAX_TEXT:
            text += "\n… (давоми бор)"
            break
        text += l + "\n"
    return text

def get_size_type_simple(value):
    """Loyiha hajmini aniqlash"""
    if pd.isna(value):
        return None
    
    val_str = str(value).lower()
    if "кичик" in val_str:
        return "kichik"
    elif "ўрта" in val_str or "орта" in val_str:
        return "orta"
    elif "йирик" in val_str:
        return "yirik"
    return None

def convert_to_float(value):
    """Qiymatni float ga o'tkazish"""
    try:
        if pd.isna(value):
            return 0.0
        clean_str = str(value).replace(" ", "").replace(",", "").strip()
        return float(clean_str) if clean_str else 0.0
    except:
        return 0.0

def parse_date(date_str):
    """Turli formatdagi sanalarni parse qilish"""
    if pd.isna(date_str) or not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Turli formatlarni sinab ko'rish
    formats = [
        '%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y',
        '%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d',
        '%d.%m.%y', '%d/%m/%y', '%d-%m-%y'
    ]
    
    for fmt_str in formats:
        try:
            return datetime.strptime(date_str, fmt_str).date()
        except:
            continue
    
    return None

# =========================
# SQLite DATABASE
# =========================

def init_db():
    """Database ni yaratish"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1. AVVAL ESKI JADVALNI O'CHIRISH (agar mavjud bo'lsa)
    cursor.execute("DROP TABLE IF EXISTS projects")

    # 2. YANGI JADVALNI TO'LIQ YARATISH
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT NOT NULL,
        korxona_turi TEXT,
        loyiha_turi TEXT,
        tuman TEXT,
        zona TEXT,
        total_value REAL,
        yearly_value REAL,
        size_type TEXT,
        hamkor TEXT,
        hamkor_mamlakat TEXT,
        holat TEXT,
        muammo TEXT,
        boshqarma_masul TEXT,
        viloyat_masul TEXT,
        muammo_muddati DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Indexlar
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_size ON projects(size_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tuman ON projects(tuman)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_korxona ON projects(korxona_turi)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_holat ON projects(holat)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_muammo ON projects(muammo)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_muddati ON projects(muammo_muddati)')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_boshqarma ON projects(boshqarma_masul)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_viloyat ON projects(viloyat_masul)')
    
    conn.commit()
    conn.close()
    print(f"✅ Database yaratildi: {DB_FILE}")

def sync_sheets_to_db():
    """Google Sheets -> SQLite (har 5 minutda)"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Ma'lumotlar yangilanmoqda...")
    
    try:
        df = get_dataframe()
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Eskilarini tozalash
        cursor.execute("DELETE FROM projects")
        
        # Yangilarini qo'shish
        records = []
        for _, row in df.iterrows():
            # Muammo muddatini formatlash
            muammo_muddati = None
            if pd.notna(row.iloc[COLUMN_INDEXES['muammo_muddati']]):
                muddat_str = str(row.iloc[COLUMN_INDEXES['muammo_muddati']])
                parsed_date = parse_date(muddat_str)
                if parsed_date:
                    muammo_muddati = parsed_date.strftime('%Y-%m-%d')
            
            records.append((
                str(row.iloc[COLUMN_INDEXES['project_name']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['project_name']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['korxona']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['korxona']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['loyiha_turi']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['loyiha_turi']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['tuman']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['tuman']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['zona']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['zona']]) else "Nomalum",
                convert_to_float(row.iloc[COLUMN_INDEXES['total_value']]),
                convert_to_float(row.iloc[COLUMN_INDEXES['yearly_value']]),
                get_size_type_simple(row.iloc[COLUMN_INDEXES['size_type']]),
                str(row.iloc[COLUMN_INDEXES['hamkor']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['hamkor']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['hamkor_mamlakat']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['hamkor_mamlakat']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['holat']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['holat']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['muammo']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['muammo']]) else "Yuq",
                str(row.iloc[COLUMN_INDEXES['boshqarma_masul']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['boshqarma_masul']]) else "Nomalum",
                str(row.iloc[COLUMN_INDEXES['viloyat_masul']]).strip() if pd.notna(row.iloc[COLUMN_INDEXES['viloyat_masul']]) else "Nomalum",
                muammo_muddati
            ))
        
        # Batch insert
        cursor.executemany('''
            INSERT INTO projects (
                project_name, korxona_turi, loyiha_turi, tuman, zona,
                total_value, yearly_value, size_type, hamkor, hamkor_mamlakat,
                holat, muammo, boshqarma_masul, viloyat_masul, muammo_muddati
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        conn.commit()
        # Jadvaldagi ma'lumotlarni tekshirish
        cursor.execute("SELECT COUNT(*) FROM projects")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM projects WHERE muammo_muddati IS NOT NULL")
        muddat_count = cursor.fetchone()[0]
        
        conn.close()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {count} ta loyiha bazaga saqlandi ✓")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {muddat_count} ta loyihada muddat mavjud")
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Xatolik: {e}")
        import traceback
        traceback.print_exc()

def start_sync_service():
    """Sinxronizatsiya servisi"""
    # Dastlabki yuklash
    sync_sheets_to_db()
    
    def sync_loop():
        while True:
            ttime.sleep(300)  # 5 minut
            sync_sheets_to_db()
    
    thread = threading.Thread(target=sync_loop, daemon=True)
    thread.start()

# =========================
# KLAVIATURALAR
# =========================

def back_btn(target="main"):
    """Orqaga tugmasi"""
    return [[InlineKeyboardButton("⬅️ Орқага", callback_data=f"back:{target}")]]

def main_menu():
    """Asosiy menyu"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Кичик", callback_data="size:kichik"),
            InlineKeyboardButton("🟡 Ўрта", callback_data="size:orta"),
            InlineKeyboardButton("🔴 Йирик", callback_data="size:yirik"),
        ],
        [InlineKeyboardButton("🏢 Корхоналар", callback_data="menu:corp")],
        [InlineKeyboardButton("🆕 Янгидан бошланадиган", callback_data="menu:new")],
        [InlineKeyboardButton("🔁 Йилдан йилга ўтувчи", callback_data="menu:cont")],
        [InlineKeyboardButton("🗂 Туманлар кесимида", callback_data="menu:district")],
        [InlineKeyboardButton("📌 Лойиҳа ҳолати", callback_data="menu:status")],
        [InlineKeyboardButton("⚠️ Муаммоли", callback_data="menu:problem")],
        [InlineKeyboardButton("📍 Муаммоли туманлар", callback_data="menu:problem_district")],
        [InlineKeyboardButton("⏰ Муаммо муддати", callback_data="menu:muddat_report")],
        [InlineKeyboardButton("👥 Ходимлар кесимида", callback_data="menu:employees")],
    ])

def pager(prefix, page, total):
    """Pagination tugmalari"""
    btns = []
    if page > 0:
        btns.append(InlineKeyboardButton("◀️ Олдинги", callback_data=f"{prefix}:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        btns.append(InlineKeyboardButton("▶️ Кейинги", callback_data=f"{prefix}:{page+1}"))
    return [btns] if btns else []

# =========================
# UMUMIY HISOBOT
# =========================

def full_report():
    """To'liq hisobot"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Asosiy statistika
        cursor.execute("SELECT COUNT(*), SUM(total_value), SUM(yearly_value) FROM projects")
        row = cursor.fetchone()
        
        if not row:
            return "⚠️ Базада маълумотлар мавжуд эмас"
        
        total_count = row[0] if row[0] is not None else 0
        total_sum = row[1] if row[1] is not None else 0
        yearly_sum = row[2] if row[2] is not None else 0
        
        # Yangi va yildan-yilga
        cursor.execute("SELECT COUNT(*), SUM(yearly_value) FROM projects WHERE loyiha_turi LIKE ?", ('%янг%',))
        new_result = cursor.fetchone()
        new_count = new_result[0] if new_result and new_result[0] is not None else 0
        new_sum = new_result[1] if new_result and new_result[1] is not None else 0
        
        cursor.execute("SELECT COUNT(*), SUM(yearly_value) FROM projects WHERE loyiha_turi LIKE ?", ('%йил%',))
        cont_result = cursor.fetchone()
        cont_count = cont_result[0] if cont_result and cont_result[0] is not None else 0
        cont_sum = cont_result[1] if cont_result and cont_result[1] is not None else 0
        
        # Size bo'yicha
        size_stats = {}
        cursor.execute("SELECT size_type, COUNT(*), SUM(total_value) FROM projects WHERE size_type IS NOT NULL GROUP BY size_type")
        for row in cursor.fetchall():
            if row[0]:
                size_stats[row[0]] = {
                    "count": row[1] if row[1] is not None else 0,
                    "sum": row[2] if row[2] is not None else 0
                }
        
        # Korxona turlari
        korxona_lines = []
        cursor.execute("SELECT korxona_turi, COUNT(*), SUM(total_value) FROM projects GROUP BY korxona_turi ORDER BY COUNT(*) DESC")
        for korxona, count, sum_val in cursor.fetchall():
            if korxona and korxona != "Nomalum":
                count_val = count if count is not None else 0
                sum_val = sum_val if sum_val is not None else 0
                korxona_lines.append(f"- {korxona}: {count_val} та, {fmt(sum_val)} млн.$")
        
        # Tumanlar
        tuman_lines = []
        cursor.execute("SELECT tuman, COUNT(*), SUM(total_value) FROM projects WHERE tuman != ? GROUP BY tuman ORDER BY tuman", ("Nomalum",))
        for tuman, count, sum_val in cursor.fetchall():
            if tuman:
                count_val = count if count is not None else 0
                sum_val = sum_val if sum_val is not None else 0
                tuman_lines.append(f"📍 {tuman}: {count_val} та, {fmt(sum_val)} млн.$")
        
        # Muammoli loyihalar soni
        cursor.execute("SELECT COUNT(*) FROM projects WHERE muammo != ? AND muammo != ? AND muammo != ?", ("Yuq", "", "Nomalum"))
        problem_result = cursor.fetchone()
        problem_count = problem_result[0] if problem_result and problem_result[0] is not None else 0
        
        lines = [
            "*Наманган вилоятида хорижий лойиҳалар дастурига хуш келибсиз!*",
            "",
            f"📊 Жами лойиҳалар: {total_count} та",
            f"💰 Жами қиймати: {fmt(total_sum)} млн.$",
            f"💰 2026 йилда ўзлаштириладиган: {fmt(yearly_sum)} млн.$",
            f"      - янгидан бошланадиган: {new_count} та, {fmt(new_sum)} млн.$",
            f"      - йилдан йилга ўтувчи: {cont_count} та, {fmt(cont_sum)} млн.$",
            "",
            "📊 *Лойиҳа ҳажми бўйича:*",
            f"  🟢 Кичик: {size_stats.get('kichik', {}).get('count', 0)} та, {fmt(size_stats.get('kichik', {}).get('sum', 0))} млн.$",
            f"  🟡 Ўрта: {size_stats.get('orta', {}).get('count', 0)} та, {fmt(size_stats.get('orta', {}).get('sum', 0))} млн.$",
            f"  🔴 Йирик: {size_stats.get('yirik', {}).get('count', 0)} та, {fmt(size_stats.get('yirik', {}).get('sum', 0))} млн.$",
            "",
            "🏢 Корхоналар:"
        ]
        
        lines.extend(korxona_lines)
        lines.append("\n🗂 Туманлар кесимида:")
        lines.extend(tuman_lines)
        lines.append(f"\n⚠️ Муаммоли лойиҳалар: {problem_count} та")
        
        return "\n".join(lines)
        
    except Exception as e:
        print(f"full_report xatolik: {e}")
        return "⚠️ Маълумотлар юкланмоқда..."
    finally:
        conn.close()

# =========================
# MUAMMO MUDDATI STATISTIKASI
# =========================

def get_muddat_stats():
    """Muammo muddati bo'yicha statistika"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        today = datetime.now().date()
        
        # 1. Jami muammolar
        cursor.execute('''
            SELECT COUNT(*) FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
        ''')
        jami_muammolar = cursor.fetchone()[0] or 0
        
        # 2. Oy bo'yicha muammolar
        oylar = {
            1: 'январ', 2: 'феврал', 3: 'март', 4: 'апрел',
            5: 'май', 6: 'июн', 7: 'июл', 8: 'август',
            9: 'сентябр', 10: 'октябр', 11: 'ноябр', 12: 'декабр'
        }
        
        oy_stats = {}
        cursor.execute('''
            SELECT 
                strftime('%m', muammo_muddati) as oy,
                COUNT(*) as soni
            FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND muammo_muddati IS NOT NULL
            GROUP BY strftime('%m', muammo_muddati)
            ORDER BY oy
        ''')
        
        for oy_num, soni in cursor.fetchall():
            if oy_num:
                oy_nomi = oylar.get(int(oy_num), f"Oy {oy_num}")
                oy_stats[oy_nomi] = soni
        
        # 3. Muddati o'tganlar
        cursor.execute('''
            SELECT COUNT(*) FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND muammo_muddati IS NOT NULL
            AND DATE(muammo_muddati) < DATE('now')
        ''')
        muddati_utgan = cursor.fetchone()[0] or 0
        
        # 4. Eng qadimgi o'tgan muddat
        cursor.execute('''
            SELECT muammo_muddati FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND muammo_muddati IS NOT NULL
            AND DATE(muammo_muddati) < DATE('now')
            ORDER BY muammo_muddati ASC
            LIMIT 1
        ''')
        
        oldest_result = cursor.fetchone()
        oldest_days = 0
        if oldest_result and oldest_result[0]:
            try:
                oldest_date = datetime.strptime(oldest_result[0], '%Y-%m-%d').date()
                oldest_days = (today - oldest_date).days
            except:
                oldest_days = 0
        
        # 5. Tezkor muammolar (3 kundan kam qolgan)
        cursor.execute('''
            SELECT COUNT(*) FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND muammo_muddati IS NOT NULL
            AND DATE(muammo_muddati) >= DATE('now')
            AND julianday(muammo_muddati) - julianday('now') <= 3
        ''')
        tezkor_muammolar = cursor.fetchone()[0] or 0
        
        # 6. Eng yaqin muddat
        cursor.execute('''
            SELECT muammo_muddati FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND muammo_muddati IS NOT NULL
            AND DATE(muammo_muddati) >= DATE('now')
            ORDER BY muammo_muddati ASC
            LIMIT 1
        ''')
        
        nearest_result = cursor.fetchone()
        qolgan_kun = 0
        if nearest_result and nearest_result[0]:
            try:
                nearest_date = datetime.strptime(nearest_result[0], '%Y-%m-%d').date()
                qolgan_kun = (nearest_date - today).days
            except:
                qolgan_kun = 0
        
        # 7. Masullar bo'yicha statistika
        masul_stats = {}
        cursor.execute('''
            SELECT boshqarma_masul, COUNT(*) as soni
            FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND boshqarma_masul != 'Nomalum'
            GROUP BY boshqarma_masul
            ORDER BY soni DESC
            LIMIT 5
        ''')
        
        for masul, soni in cursor.fetchall():
            if masul:
                masul_stats[masul] = soni
        
        conn.close()
        
        return {
            'jami_muammolar': jami_muammolar,
            'oy_stats': oy_stats,
            'muddati_utgan': muddati_utgan,
            'oldest_days': oldest_days,
            'tezkor_muammolar': tezkor_muammolar,
            'qolgan_kun': qolgan_kun,
            'masul_stats': masul_stats,
            'today': today.strftime('%d.%m.%Y')
        }
        
    except Exception as e:
        print(f"get_muddat_stats xatolik: {e}")
        conn.close()
        return None

# =========================
# XODIMLAR BO'YICHA STATISTIKA
# =========================

def get_employee_stats():
    """Xodimlar (mas'ullar) bo'yicha statistika"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # 1. Boshqarma mas'ullari
        cursor.execute('''
            SELECT 
                boshqarma_masul,
                COUNT(*) as total_projects,
                COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problem_projects,
                SUM(total_value) as total_value,
                SUM(yearly_value) as yearly_value
            FROM projects 
            WHERE boshqarma_masul != 'Nomalum' 
            GROUP BY boshqarma_masul
            ORDER BY total_projects DESC
        ''')
        
        boshqarma_stats = {}
        for masul, total, problems, total_val, yearly_val in cursor.fetchall():
            if masul:
                boshqarma_stats[masul] = {
                    'total': total or 0,
                    'problems': problems or 0,
                    'total_value': total_val or 0,
                    'yearly_value': yearly_val or 0
                }
        
        # 2. Viloyat mas'ullari
        cursor.execute('''
            SELECT 
                viloyat_masul,
                COUNT(*) as total_projects,
                COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problem_projects,
                SUM(total_value) as total_value,
                SUM(yearly_value) as yearly_value
            FROM projects 
            WHERE viloyat_masul != 'Nomalum' 
            GROUP BY viloyat_masul
            ORDER BY total_projects DESC
        ''')
        
        viloyat_stats = {}
        for masul, total, problems, total_val, yearly_val in cursor.fetchall():
            if masul:
                viloyat_stats[masul] = {
                    'total': total or 0,
                    'problems': problems or 0,
                    'total_value': total_val or 0,
                    'yearly_value': yearly_val or 0
                }
        
        # 3. Muammoli loyihalari bo'yicha top mas'ullar
        cursor.execute('''
            SELECT 
                boshqarma_masul,
                COUNT(*) as problem_count
            FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            AND boshqarma_masul != 'Nomalum'
            GROUP BY boshqarma_masul
            ORDER BY problem_count DESC
            LIMIT 10
        ''')
        
        top_problem_employees = []
        for masul, count in cursor.fetchall():
            if masul:
                top_problem_employees.append({
                    'name': masul,
                    'problem_count': count
                })
        
        conn.close()
        
        return {
            'boshqarma': boshqarma_stats,
            'viloyat': viloyat_stats,
            'top_problem': top_problem_employees
        }
        
    except Exception as e:
        print(f"get_employee_stats xatolik: {e}")
        conn.close()
        return None

# =========================
# DAILY PROBLEM REPORT
# =========================

async def daily_problem_report(context: ContextTypes.DEFAULT_TYPE):
    """Kundalik muammoli loyihalar hisoboti"""
    try:
        # 1. Umumiy statistika
        muddat_stats = get_muddat_stats()
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 2. Bugungi muammolar
        cursor.execute('''
            SELECT project_name, muammo, tuman, total_value, yearly_value, 
                   korxona_turi, size_type, holat, boshqarma_masul, 
                   viloyat_masul, muammo_muddati
            FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            ORDER BY 
                CASE 
                    WHEN muammo_muddati IS NULL THEN 1
                    WHEN DATE(muammo_muddati) < DATE('now') THEN 0
                    ELSE 2
                END,
                muammo_muddati ASC,
                total_value DESC
        ''')
        
        problems = cursor.fetchall()
        conn.close()
        
        count = len(problems)
        
        if count == 0:
            text = "✅ Бугун муаммоли лойиҳалар мавжуд эмас"
        else:
            lines = []
            
            # Umumiy statistika - HTML formatda
            if muddat_stats:
                lines.extend([
                    "<b>📊 Муаммолар ҳақида умумий маълумот:</b>",
                    "",
                    f"🔴 <b>Жами муаммолар:</b> {muddat_stats['jami_muammolar']} та",
                    f"⏰ <b>Муддати ўтганлар:</b> {muddat_stats['muddati_utgan']} та",
                    f"⚠️ <b>Тезкор муаммолар (3 кунда):</b> {muddat_stats['tezkor_muammolar']} та",
                    f"📅 <b>Энг яқин муддат:</b> {muddat_stats['qolgan_kun']} кундан сўнг",
                    "",
                    "<b>📈 Ойлар кесимида:</b>"
                ])
                
                for oy, soni in muddat_stats['oy_stats'].items():
                    lines.append(f"   • {oy.capitalize()} ойида: {soni} та")
                
                if muddat_stats['masul_stats']:
                    lines.extend([
                        "",
                        "<b>👥 Масъуллар бўйича (топ-5):</b>"
                    ])
                    for masul, soni in muddat_stats['masul_stats'].items():
                        lines.append(f"   • {masul}: {soni} та")
                
                lines.extend([
                    "",
                    "<b>📋 Бугунги муаммолар рўйхати:</b>"
                ])
            
            today = datetime.now().date()
            
            # Muammoli loyihalar ro'yxati
            for i, (loyiha, muammo, tuman, total_value, yearly_value, 
                    korxona, size_type, holat, boshqarma_masul, 
                    viloyat_masul, muammo_muddati) in enumerate(problems[:15], 1):
                
                # Size type ni o'zbekchaga o'tkazish
                size_map = {
                    "kichik": "Кичик",
                    "orta": "Ўрта",
                    "yirik": "Йирик"
                }
                size_display = size_map.get(size_type, size_type)
                
                # Muddati holati
                muddat_status = ""
                if muammo_muddati:
                    try:
                        muddat_date = datetime.strptime(muammo_muddati, '%Y-%m-%d').date()
                        qolgan_kun = (muddat_date - today).days
                        
                        if qolgan_kun < 0:
                            muddat_status = f"⛔ Муддати ўтган ({abs(qolgan_kun)} кун)"
                        elif qolgan_kun <= 3:
                            muddat_status = f"⚠️ Тезкор ({qolgan_kun} кун қолди)"
                        else:
                            muddat_status = f"📅 {qolgan_kun} кун қолди"
                    except:
                        muddat_status = "📅 Муддати белгиланган"
                else:
                    muddat_status = "❌ Муддати белгиланмаган"
                
                # HTML uchun maxsus belgilarni escape qilish
                loyiha_escaped = loyiha.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                tuman_escaped = tuman.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                korxona_escaped = korxona.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                size_display_escaped = size_display.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                holat_escaped = holat.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                boshqarma_escaped = boshqarma_masul.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                viloyat_escaped = viloyat_masul.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                muammo_escaped = muammo.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                muddat_status_escaped = muddat_status.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
                
                lines.append(
                    f"{i}) <b>{loyiha_escaped}</b>\n"
                    f"   🏙 <b>Туман:</b> {tuman_escaped}\n"
                    f"   🏢 <b>Корхона:</b> {korxona_escaped}\n"
                    f"   📏 <b>Ҳажм:</b> {size_display_escaped}\n"
                    f"   📌 <b>Ҳолати:</b> {holat_escaped}\n"
                    f"   👨‍💼 <b>Бошқармадан масъул:</b> {boshqarma_escaped}\n"
                    f"   🏛 <b>Вилоят ташкилотдан масъул:</b> {viloyat_escaped}\n"
                    f"   💰 <b>Қиймати:</b> {fmt(total_value)} млн.$\n"
                    f"   📅 <b>2026 йил ўзлаштириш:</b> {fmt(yearly_value)} млн.$\n"
                    f"   ⏰ <b>Муддат:</b> {muddat_status_escaped}\n"
                    f"   🔴 <b>Муаммоси:</b> — {muammo_escaped}"
                )
        
            text = safe_text(lines)
        
        for user_id in PROBLEM_REPORT_USERS:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"❌ Юбориб бўлмади {user_id}: {e}")
                
    except Exception as e:
        print(f"daily_problem_report xatolik: {e}")

# =========================
# MENU CALLBACK HANDLERS
# =========================

async def edit(ctx, update, text, kb):
    """Xabarni tahrirlash"""
    await ctx.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
        text=text,
        reply_markup=kb,
        parse_mode="Markdown"
    )

async def menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    print(f"DEBUG menu_cb: callback_data = {q.data}")
    
    try:
        data_parts = q.data.split(":")
        if len(data_parts) < 2:
            await q.edit_message_text(
                text="❌ Noto'g'ri format",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")]])
            )
            return
            
        key = data_parts[1]
        ctx.user_data.clear()
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Asosiy menyu item'lar uchun mapping
        key_mapping = {
            "employees": "employees",
            "muddat_report": "muddat_report",
            "problem_district": "problem_district",
            "problem": "problem",
            "status": "status",
            "district": "district",
            "corp": "corp",
            "new": "new",
            "cont": "cont",
            "expired_problems": "expired_problems",
            "urgent_problems": "urgent_problems",
            "all_deadlines": "all_deadlines",
            "boshqarma_list": "boshqarma_list",
            "viloyat_list": "viloyat_list"
        }
        
        # Agar key mapping'da bo'lsa, yangi key'ga o'tkaz
        if key in key_mapping:
            key = key_mapping[key]
        
        print(f"DEBUG: Processing key = {key}")
        
        if key == "corp":
            cursor.execute("SELECT COUNT(*) FROM projects")
            total = cursor.fetchone()[0]
            
            lines = [f"🏢 Корхоналар: {total} та жами\n"]
            
            korxona_nomlari = ["MCHJ", "QK", "XK", "Korxona ochilmagan"]
            kb_rows = []
            
            for name in korxona_nomlari:
                cursor.execute('SELECT COUNT(*), SUM(total_value) FROM projects WHERE korxona_turi = ?', (name,))
                result = cursor.fetchone()
                count = result[0] if result else 0
                sum_val = result[1] if result and result[1] else 0
                
                lines.append(f"- {name}: {count} та, {fmt(sum_val)} млн.$")
                
                if count > 0:
                    if len(kb_rows) == 0 or len(kb_rows[-1]) >= 2:
                        kb_rows.append([])
                    kb_rows[-1].append(InlineKeyboardButton(name, callback_data=f"corp:{name}"))
            
            kb_rows.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
            
            await q.edit_message_text(
                text="\n".join(lines),
                reply_markup=InlineKeyboardMarkup(kb_rows),
                parse_mode="Markdown"
            )
        
        elif key in ("new", "cont"):
            ctx.user_data["ptype"] = key
            cursor.execute("SELECT DISTINCT tuman FROM projects WHERE tuman != ? ORDER BY tuman", ("Nomalum",))
            tumanlar = [row[0] for row in cursor.fetchall()]
            
            kb = []
            for tuman in tumanlar:
                kb.append([InlineKeyboardButton(tuman, callback_data=f"dist:{tuman}:0")])
            
            kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
            
            await q.edit_message_text(
                text="🗂 *Туманни танланг:*",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode="Markdown"
            )
        
        elif key == "district":
            cursor.execute("SELECT DISTINCT tuman FROM projects WHERE tuman != ? ORDER BY tuman", ("Nomalum",))
            tumanlar = [row[0] for row in cursor.fetchall()]
            
            kb = []
            for tuman in tumanlar:
                kb.append([InlineKeyboardButton(tuman, callback_data=f"dist:{tuman}:0")])
            
            kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
            
            await q.edit_message_text(
                text="🗂 *Туманни танланг:*",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode="Markdown"
            )
        
        elif key == "status":
            cursor.execute('SELECT project_name, holat FROM projects WHERE holat != ? AND holat != ? ORDER BY total_value DESC LIMIT 30', ("Nomalum", ""))
            
            results = cursor.fetchall()
            count = len(results)
            
            if count == 0:
                await q.edit_message_text(
                    text="✅ Лойиҳалар ҳолати киритилмаган",
                    reply_markup=InlineKeyboardMarkup(back_btn("main")),
                    parse_mode="Markdown"
                )
                return
            
            lines = [f"📌 Лойиҳа ҳолати ({count} та)\n"]
            
            for i, (loyiha, holat) in enumerate(results, 1):
                lines.append(
                    f"{i}) {loyiha}\n"
                    f"     📌 *Ҳолати — {holat}*\n"
                )
            
            await q.edit_message_text(
                text=safe_text(lines),
                reply_markup=InlineKeyboardMarkup(back_btn("main")),
                parse_mode="Markdown"
            )
        
        elif key == "problem":
            # Batafsil ma'lumotlar bilan
            cursor.execute('''
                SELECT project_name, muammo, tuman, total_value, yearly_value, 
                       korxona_turi, size_type, holat, boshqarma_masul, 
                       viloyat_masul, muammo_muddati
                FROM projects 
                WHERE muammo != 'Yuq' 
                AND muammo != '' 
                AND muammo != 'Nomalum'
                ORDER BY 
                    CASE 
                        WHEN muammo_muddati IS NULL THEN 1
                        WHEN DATE(muammo_muddati) < DATE('now') THEN 0
                        ELSE 2
                    END,
                    muammo_muddati ASC,
                    total_value DESC
                LIMIT 20
            ''')
            
            results = cursor.fetchall()
            count = len(results)
            
            if count == 0:
                await q.edit_message_text(
                    text="✅ Муаммоли лойиҳалар мавжуд эмас",
                    reply_markup=InlineKeyboardMarkup(back_btn("main")),
                    parse_mode="Markdown"
                )
                return
            
            lines = [f"📕 *Муаммоли лойиҳалар:* (*{count}* та)\n"]
            
            today = datetime.now().date()
            
            for i, (loyiha, muammo, tuman, total_value, yearly_value, 
                    korxona, size_type, holat, boshqarma_masul, 
                    viloyat_masul, muammo_muddati) in enumerate(results, 1):
                
                size_map = {
                    "kichik": "Кичик",
                    "orta": "Ўрта",
                    "yirik": "Йирик"
                }
                size_display = size_map.get(size_type, size_type)
                
                # Muddati holati
                muddat_status = ""
                if muammo_muddati:
                    try:
                        muddat_date = datetime.strptime(muammo_muddati, '%Y-%m-%d').date()
                        qolgan_kun = (muddat_date - today).days
                        
                        if qolgan_kun < 0:
                            muddat_status = f"⛔ {abs(qolgan_kun)} кун муддати ўтган"
                        elif qolgan_kun <= 3:
                            muddat_status = f"⚠️ {qolgan_kun} кун қолди"
                        else:
                            muddat_status = f"📅 {qolgan_kun} кун қолди"
                    except:
                        muddat_status = "📅 Муддати белгиланган"
                else:
                    muddat_status = "❌ Муддати йўқ"
                
                lines.append(
                    f"{i}) *{loyiha}*\n"
                    f"   🏙 *Туман:* {tuman}\n"
                    f"   🏢 *Корхона:* {korxona}\n"
                    f"   📏 *Ҳажм:* {size_display}\n"
                    f"   📌 *Ҳолати:* {holat}\n"
                    f"   👨‍💼 *Бошқармадан масъул:* {boshqarma_masul}\n"
                    f"   🏛 *Вилоят ташкилотдан масъул:* {viloyat_masul}\n"
                    f"   💰 *Қиймати:* {fmt(total_value)} млн.$\n"
                    f"   📅 *2026 йил ўзлаштириш:* {fmt(yearly_value)} млн.$\n"
                    f"   ⏰ *Муддат:* {muddat_status}\n"
                    f"   🔴 *Муаммоси:* — {muammo}\n"
                    f"   {'─' * 30}\n"
                )
            
            await q.edit_message_text(
                text=safe_text(lines),
                reply_markup=InlineKeyboardMarkup(back_btn("main")),
                parse_mode="Markdown"
            )
        
        elif key == "problem_district":
            cursor.execute('''
                SELECT tuman, COUNT(*) as problem_count, 
                       SUM(total_value) as total_value_sum,
                       SUM(yearly_value) as yearly_value_sum
                FROM projects 
                WHERE muammo != 'Yuq' 
                AND muammo != '' 
                AND muammo != 'Nomalum'
                AND tuman != 'Nomalum'
                GROUP BY tuman
                ORDER BY problem_count DESC, total_value_sum DESC
                LIMIT 15
            ''')
            
            district_stats = cursor.fetchall()
            
            lines = ["⚠️ *Туманлар кесимида муаммоли лойиҳалар*\n"]
            
            keyboard = []
            
            for tuman, problem_count, total_sum, yearly_sum in district_stats:
                if tuman and problem_count > 0:
                    lines.append(
                        f"📍 *{tuman}:* {problem_count} та муаммоли лойиҳа\n"
                        f"   💰 Жами қиймати: {fmt(total_sum)} млн.$\n"
                        f"   📅 2026 йил ўзлаштириш: {fmt(yearly_sum)} млн.$\n"
                    )
                    keyboard.append([InlineKeyboardButton(
                        f"{tuman} ({problem_count} та)",
                        callback_data=f"prob_dist:{tuman}:0"
                    )])
            
            if not keyboard:
                lines.append("\n✅ Ҳеч қандай туманда муаммоли лойиҳа мавжуд эмас")
            
            keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
            
            await q.edit_message_text(
                text="\n".join(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        elif key == "muddat_report":
            muddat_stats = get_muddat_stats()
            
            if not muddat_stats:
                await q.edit_message_text(
                    text="⚠️ Муаммо муддати маълумотлари мавжуд эмас",
                    reply_markup=InlineKeyboardMarkup(back_btn("main")),
                    parse_mode="Markdown"
                )
                return
            
            today = datetime.now()
            
            lines = [
                f"⏰ *Муаммо муддати бўйича ҳисобот*",
                f"📅 *Сана:* {today.strftime('%d.%m.%Y')}",
                f"",
                f"📊 *Умумий статистика:*",
                f"🔴 Жами муаммолар: *{muddat_stats['jami_muammolar']} та*",
                f"⛔ Муддати ўтган: *{muddat_stats['muddati_utgan']} та*",
                f"⚠️ Тезкор (3 кунда): *{muddat_stats['tezkor_muammolar']} та*",
                f"📈 Муддати ўтган кун: *{muddat_stats['oldest_days']} кун*",
                f"",
                f"📅 *Яқин муддатлар:*",
                f"⏳ Энг яқин муддат: *{muddat_stats['qolgan_kun']} кундан сўнг*",
                f"",
                f"📈 *Ойлар кесимида муаммолар:*"
            ]
            
            for oy, soni in muddat_stats['oy_stats'].items():
                lines.append(f"   • {oy.capitalize()} ойида: *{soni} та*")
            
            if muddat_stats['masul_stats']:
                lines.extend([
                    f"",
                    f"👥 *Масъуллар бўйича (топ-5):*"
                ])
                for masul, soni in muddat_stats['masul_stats'].items():
                    lines.append(f"   • {masul}: *{soni} та*")
            
            keyboard = [
                [InlineKeyboardButton("⛔ Муддати ўтганлар", callback_data="menu:expired_problems")],
                [InlineKeyboardButton("⚠️ Тезкор муаммолар", callback_data="menu:urgent_problems")],
                [InlineKeyboardButton("📅 Барча муддатлилар", callback_data="menu:all_deadlines")],
                [InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")]
            ]
            
            await q.edit_message_text(
                text="\n".join(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        elif key == "expired_problems":
            await show_problems_by_status(update, ctx, "expired")
        
        elif key == "urgent_problems":
            await show_problems_by_status(update, ctx, "urgent")
        
        elif key == "all_deadlines":
            await show_problems_by_status(update, ctx, "all")
        
        elif key == "employees":
            employee_stats = get_employee_stats()
            
            if not employee_stats:
                await q.edit_message_text(
                    text="⚠️ Ходимлар маълумотлари мавжуд эмас",
                    reply_markup=InlineKeyboardMarkup(back_btn("main")),
                    parse_mode="Markdown"
                )
                return
            
            lines = [
                "👥 *Ходимлар (масъуллар) кесимида ҳисобот*",
                "",
                "🏢 *Бошқарма масъуллари:*"
            ]
            
            # Boshqarma mas'ullari
            boshqarma_count = 0
            for masul, stats in list(employee_stats['boshqarma'].items())[:10]:
                lines.append(
                    f"• {masul}: {stats['total']} та лойиҳа, "
                    f"💰 {fmt(stats['total_value'])} млн.$, "
                    f"⚠️ {stats['problems']} та муаммоли"
                )
                boshqarma_count += 1
            
            lines.extend([
                "",
                "🏛 *Вилоят ташкилот масъуллари:*"
            ])
            
            # Viloyat mas'ullari
            viloyat_count = 0
            for masul, stats in list(employee_stats['viloyat'].items())[:10]:
                lines.append(
                    f"• {masul}: {stats['total']} та лойиҳа, "
                    f"💰 {fmt(stats['total_value'])} млн.$, "
                    f"⚠️ {stats['problems']} та муаммоли"
                )
                viloyat_count += 1
            
            lines.extend([
                "",
                f"📊 *Умумий ҳисобот:*",
                f"• Бошқарма масъуллари: {len(employee_stats['boshqarma'])} та",
                f"• Вилоят масъуллари: {len(employee_stats['viloyat'])} та",
                "",
                "⚠️ *Энг кўп муаммоли лойиҳаларга масъуллар (топ-5):*"
            ])
            
            for i, emp in enumerate(employee_stats['top_problem'][:5], 1):
                lines.append(f"{i}. {emp['name']}: {emp['problem_count']} та муаммоли лойиҳа")
            
            # Tugmalar
            keyboard = [
                [InlineKeyboardButton("🏢 Бошқарма масъуллари", callback_data="menu:boshqarma_list")],
                [InlineKeyboardButton("🏛 Вилоят масъуллари", callback_data="menu:viloyat_list")],
                [InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")]
            ]
            
            await q.edit_message_text(
                text=safe_text(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        elif key == "boshqarma_list":
            # Boshqarma mas'ullari ro'yxati
            cursor.execute('''
                SELECT boshqarma_masul, COUNT(*) as total, 
                       SUM(total_value) as total_value, SUM(yearly_value) as yearly_value,
                       COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problems
                FROM projects 
                WHERE boshqarma_masul != 'Nomalum' AND boshqarma_masul != ''
                GROUP BY boshqarma_masul
                ORDER BY total DESC
                LIMIT 20
            ''')
            
            employees = cursor.fetchall()
            
            lines = ["🏢 *Бошқарма масъуллари рўйхати*\n"]
            
            keyboard = []
            
            for i, (masul, total, total_val, yearly_val, problems) in enumerate(employees, 1):
                lines.append(
                    f"{i}. {masul}\n"
                    f"   📊 Лойиҳалар: {total} та\n"
                    f"   💰 Жами қиймат: {fmt(total_val)} млн.$\n"
                    f"   📅 2026 йил ўзлаштириш: {fmt(yearly_val)} млн.$\n"
                    f"   ⚠️ Муаммоли: {problems} та\n"
                )
                
                # Tugma matnini qisqartirish
                btn_text = masul
                if len(btn_text) > 20:
                    btn_text = btn_text[:17] + "..."
                
                # callback_data'ni tozalash
                safe_name = masul.replace(":", "_").replace(";", "_")
                callback_data = f"emp_detail:bosh:{safe_name}:0"
                
                keyboard.append([
                    InlineKeyboardButton(
                        btn_text,
                        callback_data=callback_data
                    )
                ])
            
            keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:employees")])
            keyboard.append([InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")])
            
            await q.edit_message_text(
                text=safe_text(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        elif key == "viloyat_list":
            # Viloyat mas'ullari ro'yxati
            cursor.execute('''
                SELECT viloyat_masul, COUNT(*) as total, 
                       SUM(total_value) as total_value, SUM(yearly_value) as yearly_value,
                       COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problems
                FROM projects 
                WHERE viloyat_masul != 'Nomalum' AND viloyat_masul != ''
                GROUP BY viloyat_masul
                ORDER BY total DESC
                LIMIT 20
            ''')
            
            employees = cursor.fetchall()
            
            lines = ["🏛 *Вилоят масъуллари рўйхати*\n"]
            
            keyboard = []
            
            for i, (masul, total, total_val, yearly_val, problems) in enumerate(employees, 1):
                lines.append(
                    f"{i}. {masul}\n"
                    f"   📊 Лойиҳалар: {total} та\n"
                    f"   💰 Жами қиймат: {fmt(total_val)} млн.$\n"
                    f"   📅 2026 йил ўзлаштириш: {fmt(yearly_val)} млн.$\n"
                    f"   ⚠️ Муаммоли: {problems} та\n"
                )
                
                # Tugma matnini qisqartirish
                btn_text = masul
                if len(btn_text) > 20:
                    btn_text = btn_text[:17] + "..."
                
                # callback_data'ni tozalash
                safe_name = masul.replace(":", "_").replace(";", "_")
                callback_data = f"emp_detail:vil:{safe_name}:0"
                
                keyboard.append([
                    InlineKeyboardButton(
                        btn_text,
                        callback_data=callback_data
                    )
                ])
            
            keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:employees")])
            keyboard.append([InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")])
            
            await q.edit_message_text(
                text=safe_text(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        else:
            # Noma'lum key uchun
            await q.edit_message_text(
                text=f"❌ Номаълум танлов: {key}",
                reply_markup=InlineKeyboardMarkup(back_btn("main")),
                parse_mode="Markdown"
            )
    
    except Exception as e:
        print(f"menu_cb xatolik: {e}")
        import traceback
        traceback.print_exc()
        
        await q.answer(f"Xatolik: {str(e)[:30]}", show_alert=True)
            
        await q.edit_message_text(
            text=f"❌ Хатолик юз берди:\n{str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")]]),
            parse_mode="Markdown"
        )
    
    finally:
        if 'conn' in locals():
            conn.close()

async def emp_detail_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Xodim bo'yicha loyihalarni ko'rsatish"""
    q = update.callback_query
    await q.answer()
    
    try:
        data_parts = q.data.split(":")
        if len(data_parts) < 3:
            await q.answer("Noto'g'ri format", show_alert=True)
            return
        
        emp_type = data_parts[1]  # bosh yoki vil
        emp_name = data_parts[2].replace("_", " ")
        page = int(data_parts[3]) if len(data_parts) > 3 else 0
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Xodim turini aniqlash
        if emp_type == "bosh":
            column = "boshqarma_masul"
            title_prefix = "🏢"
            back_target = "menu:boshqarma_list"
        else:
            column = "viloyat_masul"
            title_prefix = "🏛"
            back_target = "menu:viloyat_list"
        
        # Xodimning to'liq ismini topish
        cursor.execute(f'''
            SELECT DISTINCT {column} FROM projects 
            WHERE {column} LIKE ? 
            LIMIT 1
        ''', (f"%{emp_name}%",))
        
        result = cursor.fetchone()
        if not result:
            await q.edit_message_text(
                text=f"❌ {emp_name} топилмади",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data=back_target)]])
            )
            conn.close()
            return
        
        full_name = result[0]
        
        # Umumiy statistika
        cursor.execute(f'''
            SELECT COUNT(*), SUM(total_value), SUM(yearly_value),
                   COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END)
            FROM projects 
            WHERE {column} = ?
        ''', (full_name,))
        
        stats = cursor.fetchone()
        total = stats[0] or 0
        total_value = stats[1] or 0
        yearly_value = stats[2] or 0
        problem_count = stats[3] or 0
        
        # Loyihalar ro'yxati
        offset = page * PAGE_SIZE
        cursor.execute(f'''
            SELECT project_name, tuman, korxona_turi, total_value, yearly_value,
                   holat, muammo, size_type, muammo_muddati
            FROM projects 
            WHERE {column} = ?
            ORDER BY total_value DESC
            LIMIT ? OFFSET ?
        ''', (full_name, PAGE_SIZE, offset))
        
        projects = cursor.fetchall()
        conn.close()
        
        today = datetime.now().date()
        
        lines = [
            f"{title_prefix} *{full_name} масъулнинг лойиҳалари*\n",
            f"📊 *Умумий статистика:*",
            f"• Жами лойиҳалар: {total} та",
            f"• Жами қиймати: {fmt(total_value)} млн.$",
            f"• 2026 йил ўзлаштириш: {fmt(yearly_value)} млн.$",
            f"• Муаммоли лойиҳалар: {problem_count} та",
            f"",
            f"📄 *Лойиҳалар рўйхати (саҳифа {page + 1}):*",
            f""
        ]
        
        for i, (project_name, tuman, korxona, total_val, yearly_val, 
                holat, muammo, size_type, muammo_muddati) in enumerate(projects, offset + 1):
            
            # Muammo holati
            muammo_status = ""
            if muammo and muammo not in ['Yuq', '', 'Nomalum']:
                if muammo_muddati:
                    try:
                        muddat_date = datetime.strptime(muammo_muddati, '%Y-%m-%d').date()
                        qolgan_kun = (muddat_date - today).days
                        if qolgan_kun < 0:
                            muammo_status = f"⛔ {abs(qolgan_kun)} кун ўтган"
                        elif qolgan_kun <= 3:
                            muammo_status = f"⚠️ {qolgan_kun} кун қолди"
                        else:
                            muammo_status = f"📅 {qolgan_kun} кун қолди"
                    except:
                        muammo_status = "⚠️ Муаммоли"
                else:
                    muammo_status = "⚠️ Муаммоли"
            else:
                muammo_status = "✅ Муаммосиз"
            
            # Loyiha nomini qisqartirish
            short_project_name = project_name
            if len(project_name) > 40:
                short_project_name = project_name[:37] + "..."
            
            lines.append(
                f"{i}. *{short_project_name}*\n"
                f"   🏙 {tuman} | 🏢 {korxona}\n"
                f"   📏 {size_type or 'Номаълум'}\n"
                f"   💰 {fmt(total_val)} млн.$ | 📅 2026: {fmt(yearly_val)} млн.$\n"
                f"   📌 Ҳолати: {holat}\n"
                f"   {muammo_status}\n"
                f"   {'─' * 30}\n"
            )
        
        keyboard = []
        
        # Pager tugmalari
        if page > 0:
            prev_callback = f"emp_detail:{emp_type}:{emp_name}:{page-1}"
            keyboard.append([InlineKeyboardButton("◀️ Олдинги", callback_data=prev_callback)])
        
        if (page + 1) * PAGE_SIZE < total:
            next_callback = f"emp_detail:{emp_type}:{emp_name}:{page+1}"
            if page > 0:
                keyboard[-1].append(InlineKeyboardButton("▶️ Кейинги", callback_data=next_callback))
            else:
                keyboard.append([InlineKeyboardButton("▶️ Кейинги", callback_data=next_callback)])
        
        # Orqaga va bosh menyu tugmalari
        keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data=back_target)])
        keyboard.append([InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")])
        
        await q.edit_message_text(
            text=safe_text(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"emp_detail_cb xatolik: {e}")
        await q.answer(f"Xatolik: {str(e)[:30]}", show_alert=True)

async def show_employee_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE, employee_type):
    """Mas'ullar ro'yxatini ko'rsatish"""
    q = update.callback_query
    await q.answer()
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        if employee_type == "boshqarma":
            column = "boshqarma_masul"
            title = "🏢 *Бошқарма масъуллари*"
            prefix = "boshqarma"
        else:
            column = "viloyat_masul"
            title = "🏛 *Вилоят ташкилот масъуллари*"
            prefix = "viloyat"
        
        cursor.execute(f'''
            SELECT 
                {column},
                COUNT(*) as total,
                SUM(total_value) as total_value,
                SUM(yearly_value) as yearly_value,
                COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problems
            FROM projects 
            WHERE {column} != 'Nomalum'
            GROUP BY {column}
            ORDER BY total DESC
        ''')
        
        employees = cursor.fetchall()
        
        lines = [f"{title}\n"]
        
        keyboard = []
        
        for i, (masul, total, total_val, yearly_val, problems) in enumerate(employees, 1):
            lines.append(
                f"{i}. *{masul}:*\n"
                f"   📊 Лойиҳалар: {total} та\n"
                f"   💰 Жами қиймат: {fmt(total_val)} млн.$\n"
                f"   📅 2026 йил ўзлаштириш: {fmt(yearly_val)} млн.$\n"
                f"   ⚠️ Муаммоли: {problems} та\n"
            )
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{masul} ({total} та)",
                    callback_data=f"employee:{prefix}:{masul}:0"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:employees")])
        
        await q.edit_message_text(
            text=safe_text(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"show_employee_list xatolik: {e}")
        await q.edit_message_text(
            text=f"❌ Xatolik: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:employees")]])
        )
    finally:
        conn.close()

async def show_problems_by_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE, status_type):
    """Muammolarni holati bo'yicha ko'rsatish"""
    q = update.callback_query
    await q.answer()
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        today = datetime.now().date()
        
        if status_type == "expired":
            title = "⛔ *Муддати ўтган муаммолар*"
            where_clause = "AND muammo_muddati IS NOT NULL AND DATE(muammo_muddati) < DATE('now')"
            order_by = "ORDER BY muammo_muddati ASC, total_value DESC"
        elif status_type == "urgent":
            title = "⚠️ *Тезкор муаммолар (3 кунда)*"
            where_clause = "AND muammo_muddati IS NOT NULL AND DATE(muammo_muddati) >= DATE('now') AND julianday(muammo_muddati) - julianday('now') <= 3"
            order_by = "ORDER BY muammo_muddati ASC, total_value DESC"
        else:
            title = "📅 *Барча муддатли муаммолар*"
            where_clause = "AND muammo_muddati IS NOT NULL"
            order_by = "ORDER BY muammo_muddati ASC, total_value DESC"
        
        cursor.execute(f'''
            SELECT project_name, muammo, tuman, total_value, yearly_value, 
                   korxona_turi, boshqarma_masul, muammo_muddati
            FROM projects 
            WHERE muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            {where_clause}
            {order_by}
            LIMIT 20
        ''')
        
        problems = cursor.fetchall()
        
        lines = [f"{title}\n"]
        
        if not problems:
            lines.append("✅ Муаммолар мавжуд эмас")
        else:
            for i, (project_name, muammo, tuman, total_value, yearly_value, 
                    korxona, boshqarma_masul, muammo_muddati) in enumerate(problems, 1):
                
                qolgan_kun = 0
                if muammo_muddati:
                    try:
                        muddat_date = datetime.strptime(muammo_muddati, '%Y-%m-%d').date()
                        qolgan_kun = (muddat_date - today).days
                    except:
                        pass
                
                lines.append(
                    f"{i}. *{project_name}*\n"
                    f"   🏙 {tuman} | 🏢 {korxona}\n"
                    f"   💰 {fmt(total_value)} млн.$ | 📅 2026: {fmt(yearly_value)} млн.$\n"
                    f"   👨‍💼 {boshqarma_masul}\n"
                    f"   ⏰ Муддат: {muammo_muddati}"
                )
                
                if qolgan_kun < 0:
                    lines.append(f"   ⛔ {abs(qolgan_kun)} кун муддати ўтган\n")
                elif qolgan_kun <= 3:
                    lines.append(f"   ⚠️ {qolgan_kun} кун қолди\n")
                else:
                    lines.append(f"   📅 {qolgan_kun} кун қолди\n")
                
                lines.append(f"   🔴 {muammo}")
                lines.append(f"   {'─' * 30}\n")
        
        keyboard = [[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:muddat_report")]]
        
        await q.edit_message_text(
            text=safe_text(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"show_problems_by_status xatolik: {e}")
        await q.edit_message_text(
            text=f"❌ Xatolik: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:muddat_report")]])
        )
    finally:
        conn.close()

# =========================
# SIZE BO'YICHA LOYIHALAR
# =========================

async def size_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    size = q.data.split(":")[1]
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*), SUM(total_value), SUM(yearly_value) 
        FROM projects WHERE size_type = ?
    ''', (size,))
    
    row = cursor.fetchone()
    if not row or row[0] == 0:
        await edit(
            ctx, update,
            f"⚠️ *{size.capitalize()} лойиҳалар топилмади*",
            InlineKeyboardMarkup(back_btn("main"))
        )
        conn.close()
        return
    
    total_count, total_n_sum, total_q_sum = row
    total_n_sum = total_n_sum or 0
    total_q_sum = total_q_sum or 0
    
    cursor.execute('''
        SELECT COUNT(DISTINCT tuman) 
        FROM projects 
        WHERE size_type = ? AND tuman != 'Nomalum'
    ''', (size,))
    district_count = cursor.fetchone()[0] or 0
    
    cursor.execute('''
        SELECT tuman, COUNT(*), SUM(total_value)
        FROM projects 
        WHERE size_type = ? AND tuman != 'Nomalum'
        GROUP BY tuman
        ORDER BY tuman
    ''', (size,))
    
    tuman_stats = cursor.fetchall()
    
    cursor.execute('''
        SELECT project_name, total_value, yearly_value 
        FROM projects 
        WHERE size_type = ? 
        ORDER BY total_value DESC 
        LIMIT 3
    ''', (size,))
    
    top_projects = cursor.fetchall()
    conn.close()
    
    size_names = {
        "kichik": "Кичик",
        "orta": "Ўрта", 
        "yirik": "Йирик"
    }
    size_name = size_names.get(size, size.capitalize())
    
    lines = [
        f"📊 *{size_name} лойиҳалар бўйича ҳисобот*\n",
        f"📌 Лойиҳалар сони: *{total_count} та*",
        f"💰 Жами қиймати: *{fmt(total_n_sum)} млн.$*",
        f"💰 2026 йилда ўзлаштириладиган қиймати: *{fmt(total_q_sum)} млн.$*",
        f"🗂 Қамраб олинган туманлар: *{district_count} та*\n",
        "🏙 *Туманлар бўйича таҳлил:*"
    ]
    
    for tuman, dist_count, dist_n_sum in tuman_stats:
        lines.append(f"📍 *{tuman}:* {dist_count} та, {fmt(dist_n_sum)} млн.$")
    
    lines.append(f"\n💰 *2026 йилда энг кўп ўзлаштириладиган {size_name} лойиҳалар:*")
    
    for i, (project_name, n_value, q_value) in enumerate(top_projects, 1):
        lines.append(
            f"{i}. *{project_name}* — {fmt(n_value)} млн.$, 2026 йил ўзлаштириш — {fmt(q_value)} млн.$"
        )
    
    kb = []
    for tuman, dist_count, _ in tuman_stats:
        kb.append([
            InlineKeyboardButton(
                tuman,
                callback_data=f"sizeDist:{size}:{tuman}:0"
            )
        ])
    
    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
    
    ctx.user_data.clear()
    ctx.user_data["size"] = size
    
    await edit(ctx, update, safe_text(lines), InlineKeyboardMarkup(kb))

async def size_dist_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    _, size, district, page = q.data.split(":")
    page = int(page)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*), SUM(total_value), SUM(yearly_value) 
        FROM projects 
        WHERE size_type = ? AND tuman = ?
    ''', (size, district))
    
    row = cursor.fetchone()
    if not row or row[0] == 0:
        await edit(
            ctx, update,
            f"⚠️ *{district} туманида {size} лойиҳалар топилмади*",
            InlineKeyboardMarkup([InlineKeyboardButton("⬅️ Орқага", callback_data=f"size:{size}")])
        )
        conn.close()
        return
    
    total, total_n_sum, total_q_sum = row
    total_n_sum = total_n_sum or 0
    total_q_sum = total_q_sum or 0
    
    offset = page * PAGE_SIZE
    cursor.execute('''
        SELECT project_name, korxona_turi, total_value, yearly_value, 
               holat, muammo, zona, hamkor, hamkor_mamlakat, size_type
        FROM projects 
        WHERE size_type = ? AND tuman = ?
        ORDER BY total_value DESC
        LIMIT ? OFFSET ?
    ''', (size, district, PAGE_SIZE, offset))
    
    projects = cursor.fetchall()
    
    cursor.execute('''
        SELECT korxona_turi, COUNT(*), SUM(total_value)
        FROM projects 
        WHERE size_type = ? AND tuman = ?
        GROUP BY korxona_turi
    ''', (size, district))
    
    corp_stats = cursor.fetchall()
    conn.close()
    
    size_names = {
        "kichik": "Кичик",
        "orta": "Ўрта", 
        "yirik": "Йирик"
    }
    size_name = size_names.get(size, size.capitalize())
    
    lines = [f"📄 *{district} тумани — {size_name} лойиҳалар* ({total} та)\n"]
    
    lines.append(f"💰 *{size_name} лойиҳалар жами қиймати:* {fmt(total_n_sum)} млн.$")
    lines.append(f"  - *2026 йилда ўзлаштириладиган қиймати:* {fmt(total_q_sum)} млн.$")
    
    if corp_stats:
        lines.append("\n🏢 *Корхона турлари бўйича:*")
        for corp_type, count, sum_n in corp_stats:
            lines.append(f"  - *{corp_type}:* {count} та, {fmt(sum_n)} млн.$")
    
    lines.append("\n" + "="*40 + "\n")

    for i, project in enumerate(projects, offset + 1):
        (project_name, corp_type, n_value, q_value, 
         status, problem, zone, partner, partner_country, size_type_display) = project
        
        lines.append(
            f"*{i}. {project_name}*\n"
            f"   🏢 *Корхона:* {corp_type}\n"
            f"   📏 *Лойиҳа ҳажми:* {size_type_display}\n"
            f"   💰 *Қиймати:* {fmt(n_value)} млн.$\n"
            f"     - *2026 йилда ўзлаштириш:* {fmt(q_value)} млн.$\n"
            f"   📌 *Ҳолати:* {status}\n"
            f"   ⚠️ *Муаммо:* {problem}\n"
            f"   🏭 *Зона:* {zone}\n"
            f"   🌍 *Ҳамкор:* {partner} ({partner_country})\n"
            f"   {'─'*30}"
        )

    kb = pager(f"sizeDist:{size}:{district}", page, total)
    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data=f"size:{size}")])

    await edit(ctx, update, safe_text(lines), InlineKeyboardMarkup(kb))

# =========================
# KORXONA → TUMAN → LOYIHA
# =========================

async def corp_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    corp = q.data.split(":")[1]
    ctx.user_data.clear()
    ctx.user_data["corp"] = corp

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT tuman, COUNT(*), SUM(total_value)
        FROM projects 
        WHERE korxona_turi = ? AND tuman != 'Nomalum'
        GROUP BY tuman
        ORDER BY tuman
    ''', (corp,))
    
    tuman_stats = cursor.fetchall()
    conn.close()
    
    lines = [f"🗂 *{corp} — туманлар кесимида*\n"]

    kb = []
    for tuman, cnt, dist_n_sum in tuman_stats:
        lines.append(f"📍 *{tuman}:* {cnt} та, {fmt(dist_n_sum)} млн.$")
        kb.append([InlineKeyboardButton(tuman, callback_data=f"corpdist:{corp}:{tuman}:0")])

    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:corp")])

    await edit(
        ctx,
        update,
        safe_text(lines),
        InlineKeyboardMarkup(kb)
    )

async def corpdist_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    _, corp, district, page = q.data.split(":")
    page = int(page)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*)
        FROM projects 
        WHERE korxona_turi = ? AND tuman = ?
    ''', (corp, district))
    
    total = cursor.fetchone()[0] or 0
    
    offset = page * PAGE_SIZE
    cursor.execute('''
        SELECT project_name, total_value, yearly_value, holat, muammo, zona, hamkor, hamkor_mamlakat
        FROM projects 
        WHERE korxona_turi = ? AND tuman = ?
        ORDER BY total_value DESC
        LIMIT ? OFFSET ?
    ''', (corp, district, PAGE_SIZE, offset))
    
    projects = cursor.fetchall()
    conn.close()
    
    lines = [f"📄 *{district} — {corp}* ({total} та)\n"]

    for i, project in enumerate(projects, page * PAGE_SIZE + 1):
        (project_name, n_value, q_value, status, 
         problem, zone, partner, partner_country) = project
        
        lines.append(
            f"*{i}. {project_name}*\n"
            f"   💰 *Қиймати:* {fmt(n_value)} млн.$\n"
            f"     - *2026 йилда ўзлаштириш:* {fmt(q_value)} млн.$\n"
            f"   📌 *Ҳолати:* {status}\n"
            f"   ⚠️ *Муаммо:* {problem}\n"
            f"   🏭 *Зона:* {zone}\n"
            f"   🌍 *Ҳамкор:* {partner} ({partner_country})\n"
        )

    kb = []
    kb += pager(f"corpdist:{corp}:{district}", page, total)
    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data=f"corp:{corp}")])

    await edit(
        ctx,
        update,
        safe_text(lines),
        InlineKeyboardMarkup(kb)
    )

# =========================
# TUMANLAR → PROYEKTLAR
# =========================

async def show_districts(update, ctx, df=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT tuman FROM projects WHERE tuman != 'Nomalum' ORDER BY tuman")
    tumanlar = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    kb = []
    for d in tumanlar:
        kb.append([InlineKeyboardButton(d, callback_data=f"dist:{d}:0")])
    
    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")])
    
    await edit(ctx, update, "🗂 *Туманни танланг:*", InlineKeyboardMarkup(kb))

async def dist_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    if len(data_parts) == 3:
        _, district, page = data_parts
        page = int(page)
        korxona = None
    else:
        _, district, page, korxona = data_parts
        page = int(page)
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    where_clause = "tuman = ?"
    params = [district]
    
    if korxona:
        where_clause += " AND korxona_turi = ?"
        params.append(korxona)
    
    if ctx.user_data.get("ptype") == "new":
        where_clause += " AND loyiha_turi LIKE '%янг%'"
    elif ctx.user_data.get("ptype") == "cont":
        where_clause += " AND loyiha_turi LIKE '%йил%'"
    
    cursor.execute(f'''
        SELECT COUNT(*), SUM(total_value), SUM(yearly_value)
        FROM projects 
        WHERE {where_clause}
    ''', params)
    
    row = cursor.fetchone()
    total, total_n_sum, total_q_sum = row if row else (0, 0, 0)
    total_n_sum = total_n_sum or 0
    total_q_sum = total_q_sum or 0
    
    cursor.execute(f'''
        SELECT DISTINCT korxona_turi
        FROM projects 
        WHERE {where_clause}
    ''', params)
    
    corp_types = [row[0] for row in cursor.fetchall() if row[0] and row[0] != "Nomalum"]
    
    offset = page * PAGE_SIZE
    cursor.execute(f'''
        SELECT project_name, korxona_turi, total_value, yearly_value, 
               holat, muammo, zona, hamkor, hamkor_mamlakat
        FROM projects 
        WHERE {where_clause}
        ORDER BY total_value DESC
        LIMIT ? OFFSET ?
    ''', params + [PAGE_SIZE, offset])
    
    projects = cursor.fetchall()
    conn.close()
    
    lines = [
        f"📊 *{district} туманидаги лойиҳалар*\n",
        f"📌 Жами лойиҳалар: {total} та",
        f"💰 Жами қиймати: {fmt(total_n_sum)} млн.$",
        f"💰 2026 йилда ўзлаштириладиган қиймати: {fmt(total_q_sum)} млн.$",
    ]
    
    if corp_types:
        lines.append(f"🏢 Корхона турлари: {', '.join(corp_types)}\n")
    else:
        lines.append("")

    for i, project in enumerate(projects, page * PAGE_SIZE + 1):
        (project_name, corp_type, n_value, q_value, 
         status, problem, zone, partner, partner_country) = project
        
        lines.append(
            f"*{i}. {project_name}*\n"
            f"   🏢 *Корхона:* {corp_type}\n"
            f"   💰 *Қиймати:* {fmt(n_value)} млн.$\n"
            f"     - *2026 йилда ўзлаштириш:* {fmt(q_value)} млн.$\n"
            f"   📌 *Ҳолати:* {status}\n"
            f"   ⚠️ *Муаммо:* {problem}\n"
            f"   🏭 *Зона:* {zone}\n"
            f"   🌍 *Ҳамкор:* {partner} ({partner_country})\n"
        )

    if korxona:
        prefix = f"dist:{district}:{korxona}"
    else:
        prefix = f"dist:{district}"
    
    kb = pager(prefix, page, total)
    kb.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:district")])

    await edit(ctx, update, safe_text(lines), InlineKeyboardMarkup(kb))

# =========================
# TUMANLARDA MUAMMOLI LOYIHALAR
# =========================

async def problem_district_detail_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    tuman = data_parts[1]
    page = int(data_parts[2]) if len(data_parts) > 2 else 0
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT COUNT(*), SUM(total_value), SUM(yearly_value)
            FROM projects 
            WHERE tuman = ? 
            AND muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
        ''', (tuman,))
        
        result = cursor.fetchone()
        total_count = result[0] if result else 0
        total_sum = result[1] if result and result[1] else 0
        yearly_sum = result[2] if result and result[2] else 0
        
        if total_count == 0:
            await q.edit_message_text(
                text=f"📍 *{tuman} туманида муаммоли лойиҳалар мавжуд эмас*",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:problem_district")]]),
                parse_mode="Markdown"
            )
            return
        
        offset = page * PAGE_SIZE
        cursor.execute('''
            SELECT project_name, muammo, total_value, yearly_value, 
                   korxona_turi, size_type, holat, zona, hamkor, hamkor_mamlakat
            FROM projects 
            WHERE tuman = ? 
            AND muammo != 'Yuq' 
            AND muammo != '' 
            AND muammo != 'Nomalum'
            ORDER BY total_value DESC
            LIMIT ? OFFSET ?
        ''', (tuman, PAGE_SIZE, offset))
        
        projects = cursor.fetchall()
        
        lines = [
            f"📍 *{tuman} тумани - муаммоли лойиҳалар*\n",
            f"📌 Жами муаммоли лойиҳалар: {total_count} та",
            f"💰 Жами қиймати: {fmt(total_sum)} млн.$",
            f"📅 2026 йил ўзлаштириш: {fmt(yearly_sum)} млн.$",
            f"\n📄 Саҳифа {page + 1}/{(total_count + PAGE_SIZE - 1) // PAGE_SIZE}\n"
        ]
        
        for i, project in enumerate(projects, offset + 1):
            (project_name, muammo, total_value, yearly_value, 
             korxona, size_type, holat, zona, partner, partner_country) = project
            
            size_map = {
                "kichik": "Кичик",
                "orta": "Ўрта", 
                "yirik": "Йирик"
            }
            size_display = size_map.get(size_type, size_type)
            
            lines.append(
                f"{i}. *{project_name}*\n"
                f"   🏢 *Корхона:* {korxona}\n"
                f"   📏 *Ҳажм:* {size_display}\n"
                f"   📌 *Ҳолати:* {holat}\n"
                f"   💰 *Қиймати:* {fmt(total_value)} млн.$\n"
                f"   📅 *2026 йил ўзлаштириш:* {fmt(yearly_value)} млн.$\n"
                f"   🏭 *Зона:* {zona}\n"
                f"   🌍 *Ҳамкор:* {partner} ({partner_country})\n"
                f"   🔴 *Муаммоси:* {muammo}\n"
                f"   {'─' * 30}\n"
            )
        
        keyboard = []
        if page > 0:
            keyboard.append(InlineKeyboardButton("◀️ Олдинги", callback_data=f"prob_dist:{tuman}:{page-1}"))
        if (page + 1) * PAGE_SIZE < total_count:
            keyboard.append(InlineKeyboardButton("▶️ Кейинги", callback_data=f"prob_dist:{tuman}:{page+1}"))
        
        if keyboard:
            keyboard = [keyboard]
        
        keyboard.append([InlineKeyboardButton("⬅️ Туманларга", callback_data="menu:problem_district")])
        keyboard.append([InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")])
        
        await q.edit_message_text(
            text=safe_text(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"problem_district_detail_cb xatolik: {e}")
        await q.edit_message_text(
            text=f"❌ Xatolik: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:problem_district")]])
        )
    finally:
        conn.close()

# =========================
# XODIMLAR BO'YICHA LOYIHALAR
# =========================

async def employee_projects_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mas'ul bo'yicha loyihalarni ko'rsatish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    employee_type = data_parts[1]  # boshqarma yoki viloyat
    employee_name = data_parts[2]
    page = int(data_parts[3]) if len(data_parts) > 3 else 0
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        if employee_type == "boshqarma":
            column = "boshqarma_masul"
            title = f"🏢 *{employee_name} масъули*"
        else:
            column = "viloyat_masul"
            title = f"🏛 *{employee_name} масъули*"
        
        # Umumiy statistika
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total,
                SUM(total_value) as total_value,
                SUM(yearly_value) as yearly_value,
                COUNT(CASE WHEN muammo != 'Yuq' AND muammo != '' AND muammo != 'Nomalum' THEN 1 END) as problems,
                COUNT(CASE WHEN muammo_muddati IS NOT NULL AND DATE(muammo_muddati) < DATE('now') THEN 1 END) as expired_problems
            FROM projects 
            WHERE {column} = ?
        ''', (employee_name,))
        
        stats = cursor.fetchone()
        total = stats[0] or 0
        total_value = stats[1] or 0
        yearly_value = stats[2] or 0
        problems = stats[3] or 0
        expired_problems = stats[4] or 0
        
        # Loyihalar ro'yxati
        offset = page * PAGE_SIZE
        cursor.execute(f'''
            SELECT 
                project_name, tuman, korxona_turi, total_value, yearly_value,
                holat, muammo, size_type, muammo_muddati
            FROM projects 
            WHERE {column} = ?
            ORDER BY total_value DESC
            LIMIT ? OFFSET ?
        ''', (employee_name, PAGE_SIZE, offset))
        
        projects = cursor.fetchall()
        
        today = datetime.now().date()
        
        lines = [
            f"{title}",
            f"",
            f"📊 *Умумий статистика:*",
            f"• Жами лойиҳалар: *{total} та*",
            f"• Жами қиймати: *{fmt(total_value)} млн.$*",
            f"• 2026 йил ўзлаштириш: *{fmt(yearly_value)} млн.$*",
            f"• Муаммоли лойиҳалар: *{problems} та*",
            f"• Муддати ўтган муаммолар: *{expired_problems} та*",
            f"",
            f"📄 *Лойиҳалар рўйхати (саҳифа {page + 1}):*",
            f""
        ]
        
        for i, (project_name, tuman, korxona, total_val, yearly_val, 
                holat, muammo, size_type, muammo_muddati) in enumerate(projects, offset + 1):
            
            # Muammo holati
            muammo_status = ""
            if muammo and muammo not in ['Yuq', '', 'Nomalum']:
                if muammo_muddati:
                    try:
                        muddat_date = datetime.strptime(muammo_muddati, '%Y-%m-%d').date()
                        qolgan_kun = (muddat_date - today).days
                        if qolgan_kun < 0:
                            muammo_status = f"⛔ {abs(qolgan_kun)} кун ўтган"
                        elif qolgan_kun <= 3:
                            muammo_status = f"⚠️ {qolgan_kun} кун қолди"
                        else:
                            muammo_status = f"📅 {qolgan_kun} кун қолди"
                    except:
                        muammo_status = "⚠️ Муаммоли"
                else:
                    muammo_status = "⚠️ Муаммоли"
            else:
                muammo_status = "✅ Муаммосиз"
            
            lines.append(
                f"{i}. *{project_name}*\n"
                f"   🏙 {tuman} | 🏢 {korxona}\n"
                f"   📏 {size_type or 'Номаълум'}\n"
                f"   💰 {fmt(total_val)} млн.$ | 📅 2026: {fmt(yearly_val)} млн.$\n"
                f"   📌 Ҳолати: {holat}\n"
                f"   {muammo_status}\n"
                f"   {'─' * 30}\n"
            )
        
        keyboard = []
        if page > 0:
            keyboard.append([InlineKeyboardButton("◀️ Олдинги", callback_data=f"employee:{employee_type}:{employee_name}:{page-1}")])
        if (page + 1) * PAGE_SIZE < total:
            keyboard.append([InlineKeyboardButton("▶️ Кейинги", callback_data=f"employee:{employee_type}:{employee_name}:{page+1}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data=f"menu:{'boshqarma_list' if employee_type == 'boshqarma' else 'viloyat_list'}")])
        keyboard.append([InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")])
        
        await q.edit_message_text(
            text=safe_text(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"employee_projects_cb xatolik: {e}")
        await q.edit_message_text(
            text=f"❌ Xatolik: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:employees")]])
        )
    finally:
        conn.close()

# =========================
# BACK HANDLER
# =========================

async def back_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    target = q.data.split(":")[1] if ":" in q.data else "main"
        
    if target == "main":
        await q.edit_message_text(
            text=full_report(),
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )
    elif target == "corp":
        await menu_cb(update, ctx)
    elif target == "district":
        await show_districts(update, ctx)

# =========================
# START
# =========================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in USERS:
        await update.message.reply_text("⛔ Рухсат йўқ")
        return
    
    # Database borligini tekshirish
    if not os.path.exists(DB_FILE):
        msg = await update.message.reply_text("🔄 Маълумотлар юкланмоқда, бироз кутінг...")
        sync_sheets_to_db()
        await msg.delete()

    await update.message.reply_text(
        full_report(),
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )

# =========================
# ERROR HANDLER
# =========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """ Xatoliklar uchun handler """
    print(f"❌ Xatolik: {context.error}")
    
    if isinstance(update, Update) and update.callback_query:
        try:
            await update.callback_query.answer(f"Xatolik: {str(context.error)[:50]}")
            await update.callback_query.edit_message_text(
                text="⚠️ Хатолик юз берди. Илтимос, қайта уриниб кўринг.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")]])
            )
        except:
            pass

# =========================
# MAIN
# =========================

def main():
    # Database ni yaratish
    init_db()
    
    # Sinxronizatsiyani ishga tushirish
    start_sync_service()
    
    # Bot
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Error handler
    app.add_error_handler(error_handler)

    # ===== COMMAND & CALLBACK HANDLERS =====
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(menu_cb, pattern="^menu:"))  # Eski pattern
    app.add_handler(CallbackQueryHandler(emp_detail_cb, pattern="^emp_detail:"))  # Yangi xodim detali
    app.add_handler(CallbackQueryHandler(corp_cb, pattern="^corp:"))
    app.add_handler(CallbackQueryHandler(dist_cb, pattern="^dist:"))
    app.add_handler(CallbackQueryHandler(corpdist_cb, pattern="^corpdist:"))
    app.add_handler(CallbackQueryHandler(back_cb, pattern="^back:"))
    app.add_handler(CallbackQueryHandler(size_cb, pattern="^size:"))
    app.add_handler(CallbackQueryHandler(size_dist_cb, pattern="^sizeDist:"))
    app.add_handler(CallbackQueryHandler(problem_district_detail_cb, pattern="^prob_dist:"))
    app.add_handler(CallbackQueryHandler(employee_projects_cb, pattern="^employee:"))

    # ===== DAILY PROBLEM REPORT =====
    # APScheduler o'rniga oddiy ishlatamiz
    import asyncio
    
    async def schedule_daily_report():
        """ Kunlik hisobotni jo'natish """
        while True:
            now = datetime.now(pytz.timezone('Asia/Tashkent'))
            # Har kuni soat 17:00 da
            if now.hour == 17 and now.minute == 59:
                await daily_problem_report(app)
                # Keyingi kunga qadar kutish
                await asyncio.sleep(60 * 60 * 24 - 60)  # 23 soat 59 minut
            else:
                # Har minut tekshirish
                await asyncio.sleep(60)
    
    # Background task
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(schedule_daily_report())
    except:
        print("⚠️ Daily report scheduler ishlamadi, lekin bot ishlaydi")

    print("\n" + "="*50)
    print("🤖 BOT ISHGA TUSHDI! (SQL versiya)")
    print("="*50)
    print("📊 Google Sheets har 5 minutda SQLite bazaga yangilanadi")
    print(f"💾 Database fayli: {DB_FILE}")
    print("⏰ Kunlik hisobot har kuni soat 19:00 da")
    print("👥 Xodimlar kesmida yangi funksiya qo'shildi")
    print("✅ /start buyrug'ini tekshiring\n")
    
    app.run_polling()

if __name__ == "__main__":
    main()
