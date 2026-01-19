from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import sqlite3
import threading
import time as ttime
from datetime import datetime, time, timedelta
import pandas as pd
import os
import pytz
import asyncio
import json
from typing import Dict, List, Optional

from config import BOT_TOKEN
from users import USERS, PROBLEM_REPORT_USERS
from sheets import get_dataframe
from sheets import get_daily_works  # get_dataframe dan keyin qo'shing
# =========================
# KONSTANTALAR
# =========================

CACHE_TTL = 60          # sekund
PAGE_SIZE = 5           # loyiha soni (pagination)
MAX_TEXT = 3800         # Telegram limit
DB_FILE = "projects.db"
DAILY_PLANS_FILE = "daily_plans.json"

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
    'holat': 27,             # AB - Loyiha holati
    'muammo': 28,            # AC - Muammo
    'boshqarma_masul': 29,   # AD - Boshqarmadan masul
    'viloyat_masul': 30,     # AE - Viloyat tashkilotdan masul
    'muammo_muddati': 32     # AG - Muammo muddati
}

# =========================
# DAILY PLANS (KUNLIK ISH REJALARI)
# =========================

# =========================
# DAILY PLANS (KUNLIK ISH REJALARI) - ROLLAR BILAN
# =========================

class DailyPlans:
    """Kunlik ish rejalarini boshqarish"""
    
    def __init__(self, file_path: str = DAILY_PLANS_FILE):
        self.file_path = file_path
        self.data = self._load_data()
    
    def _load_data(self) -> Dict:
        """Ma'lumotlarni yuklash"""
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except:
            pass
        return {}
    
    def _save_data(self):
        """Ma'lumotlarni saqlash"""
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Kunlik rejalarni saqlash xatosi: {e}")
    
    def add_plan(self, user_id: int, plan_text: str, due_date: str = None, plan_date: str = None):
        """Yangi reja qo'shish (muddat bilan)"""
        if plan_date is None:
            plan_date = datetime.now().strftime('%Y-%m-%d')
        
        if plan_date not in self.data:
            self.data[plan_date] = {}
        
        if str(user_id) not in self.data[plan_date]:
            self.data[plan_date][str(user_id)] = []
        
        plan_id = len(self.data[plan_date][str(user_id)]) + 1
        plan = {
            'id': plan_id,
            'text': plan_text,
            'due_date': due_date,  # Muddati
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user_id': user_id,  # Kim qo'shganligi
            'completed': False,
            'notified': False  # Eslatma yuborilganligi
        }
        
        self.data[plan_date][str(user_id)].append(plan)
        self._save_data()
        return plan_id
    
    def get_plans(self, user_id: int, plan_date: str = None, viewer_id: int = None):
        """Kunlik rejalarni olish (rolga qarab)"""
        if plan_date is None:
            plan_date = datetime.now().strftime('%Y-%m-%d')
        
        if plan_date not in self.data:
            return []
        
        # Agar admin ko'rmoqchi bo'lsa, barcha rejalarni qaytarish
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
            # Admin uchun barcha rejalar
            all_plans = []
            for user_id_str, plans in self.data[plan_date].items():
                all_plans.extend(plans)
            return all_plans
        else:
            # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
            return self.data[plan_date].get(str(user_id), [])
    
    def get_user_plans(self, user_id: int, plan_date: str = None):
        """Faqat o'z rejalarini olish"""
        if plan_date is None:
            plan_date = datetime.now().strftime('%Y-%m-%d')
        
        if plan_date not in self.data:
            return []
        
        return self.data[plan_date].get(str(user_id), [])
    
    def get_all_plans_for_admin(self, plan_date: str = None):
        """Admin uchun barcha rejalarni olish"""
        if plan_date is None:
            plan_date = datetime.now().strftime('%Y-%m-%d')
        
        if plan_date not in self.data:
            return []
        
        all_plans = []
        for user_id_str, plans in self.data[plan_date].items():
            for plan in plans:
                plan['owner_user_id'] = int(user_id_str)  # Kimga tegishli ekanligini qo'shamiz
                all_plans.append(plan)
        
        return all_plans
    
    def get_upcoming_plans(self, user_id: int, viewer_id: int = None):
        """Kelajakdagi (muddati bor) rejalarni olish"""
        upcoming = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        for date_key, date_data in self.data.items():
            # Admin uchun barcha rejalar
            if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
                for user_id_str, plans in date_data.items():
                    for plan in plans:
                        if plan.get('due_date') and not plan.get('completed', False):
                            upcoming.append({
                                'date': date_key,
                                'plan': plan,
                                'owner_user_id': int(user_id_str)
                            })
            else:
                # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
                if str(user_id) in date_data:
                    for plan in date_data[str(user_id)]:
                        if plan.get('due_date') and not plan.get('completed', False):
                            upcoming.append({
                                'date': date_key,
                                'plan': plan,
                                'owner_user_id': user_id
                            })
        
        # Muddat bo'yicha tartiblash
        upcoming.sort(key=lambda x: x['plan']['due_date'] if x['plan']['due_date'] else '9999-99-99')
        return upcoming
    
    def get_today_plans_with_due_date(self, viewer_id: int = None):
        """Bugungi muddati kelgan rejalarni olish"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_plans = []
        
        for date_key, date_data in self.data.items():
            for user_id_str, plans in date_data.items():
                for plan in plans:
                    if plan.get('due_date') == today and not plan.get('completed', False):
                        today_plans.append({
                            'user_id': int(user_id_str),
                            'date': date_key,
                            'plan': plan
                        })
        
        return today_plans
    
    def get_all_plans_today(self, viewer_id: int = None):
        """Bugungi barcha rejalarni olish"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
            # Admin uchun barcha rejalar
            all_plans = {}
            for user_id_str, plans in self.data.get(today, {}).items():
                all_plans[user_id_str] = plans
            return all_plans
        else:
            # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
            return self.data.get(today, {})
    
    def toggle_plan(self, user_id: int, plan_date: str, plan_id: int, viewer_id: int = None):
        """Reja holatini o'zgartirish (faqat o'z rejalarini)"""
        if plan_date not in self.data:
            return False
        
        # Admin har qanday rejani o'zgartira oladi
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
            # Admin har qanday rejani o'zgartira oladi
            for user_id_str, plans in self.data[plan_date].items():
                for plan in plans:
                    if plan['id'] == plan_id:
                        plan['completed'] = not plan['completed']
                        plan['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if plan['completed'] else None
                        plan['completed_by'] = viewer_id  # Kim bajardi
                        self._save_data()
                        return True
        
        # Oddiy foydalanuvchi faqat o'z rejalarini o'zgartira oladi
        if str(user_id) not in self.data[plan_date]:
            return False
        
        for plan in self.data[plan_date][str(user_id)]:
            if plan['id'] == plan_id:
                plan['completed'] = not plan['completed']
                plan['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if plan['completed'] else None
                plan['completed_by'] = user_id  # Kim bajardi
                self._save_data()
                return True
        
        return False
    
    def delete_plan(self, user_id: int, plan_date: str, plan_id: int, viewer_id: int = None):
        """Rejani o'chirish (faqat o'z rejalarini)"""
        if plan_date not in self.data:
            return False
        
        # Admin har qanday rejani o'chira oladi
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
            for user_id_str, user_plans in self.data[plan_date].items():
                for i, plan in enumerate(user_plans):
                    if plan['id'] == plan_id:
                        del user_plans[i]
                        # ID larni qayta tartiblash
                        for j, p in enumerate(user_plans, 1):
                            p['id'] = j
                        self._save_data()
                        return True
        
        # Oddiy foydalanuvchi faqat o'z rejalarini o'chira oladi
        if str(user_id) not in self.data[plan_date]:
            return False
        
        user_plans = self.data[plan_date][str(user_id)]
        for i, plan in enumerate(user_plans):
            if plan['id'] == plan_id:
                del user_plans[i]
                # ID larni qayta tartiblash
                for j, p in enumerate(user_plans, 1):
                    p['id'] = j
                self._save_data()
                return True
        
        return False
    
    def clear_plans(self, user_id: int, plan_date: str = None, viewer_id: int = None):
        """Barcha rejalarni tozalash (faqat o'z rejalarini)"""
        if plan_date is None:
            plan_date = datetime.now().strftime('%Y-%m-%d')
        
        # Admin faqat o'zining rejalarini tozalay oladi yoki barchasini
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin' and viewer_id == user_id:
            # Admin o'z rejalarini tozalaydi
            if plan_date in self.data and str(user_id) in self.data[plan_date]:
                del self.data[plan_date][str(user_id)]
                if not self.data[plan_date]:
                    del self.data[plan_date]
                self._save_data()
                return True
        
        # Oddiy foydalanuvchi faqat o'z rejalarini tozalay oladi
        if plan_date in self.data and str(user_id) in self.data[plan_date]:
            del self.data[plan_date][str(user_id)]
            if not self.data[plan_date]:
                del self.data[plan_date]
            self._save_data()
            return True
        
        return False
    
    def get_stats(self, user_id: int, plan_date: str = None, viewer_id: int = None):
        """Statistika olish (rolga qarab)"""
        if viewer_id and viewer_id in USERS and USERS[viewer_id].get('role') == 'admin':
            # Admin uchun barcha rejalar statistikasi
            plans = self.get_all_plans_for_admin(plan_date)
        else:
            # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
            plans = self.get_user_plans(user_id, plan_date)
        
        total = len(plans)
        completed = len([p for p in plans if p['completed']])
        return total, completed

# Kunlik rejalar obyekti
daily_plans = DailyPlans()

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

def is_valid_date(date_str):
    """Sana to'g'ri formatda ekanligini tekshirish"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except:
        return False

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
        [InlineKeyboardButton("📅 Кунлик иш режалари", callback_data="menu:daily_plans")],
        [InlineKeyboardButton("📋 Kunlik ishlar (Excel)", callback_data="daily_works:view")],  # YANGI TUGMA
    ])

def pager(prefix, page, total):
    """Pagination tugmalari"""
    btns = []
    if page > 0:
        btns.append(InlineKeyboardButton("◀️ Олдинги", callback_data=f"{prefix}:{page-1}"))
    if (page + 1) * PAGE_SIZE < total:
        btns.append(InlineKeyboardButton("▶️ Кейинги", callback_data=f"{prefix}:{page+1}"))
    return [btns] if btns else []

def daily_plans_menu():
    """Kunlik rejalar menyusi"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
        [InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")],
        [InlineKeyboardButton("📅 Келажакдаги режаларим", callback_data="daily:upcoming")],
        [InlineKeyboardButton("📊 Бугунги статистика", callback_data="daily:stats")],
        [InlineKeyboardButton("⏰ Бугун муддати", callback_data="daily:today_due")],
        [InlineKeyboardButton("🧹 Режаларимни тозалаш", callback_data="daily:clear")],
        [InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")]
    ])

def plan_actions_menu(plan_date: str, plan_id: int):
    """Reja uchun amallar menyusi"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Бажарилди", callback_data=f"daily:toggle:{plan_date}:{plan_id}"),
            InlineKeyboardButton("❌ Ўчириш", callback_data=f"daily:delete:{plan_date}:{plan_id}")
        ],
        [InlineKeyboardButton("⬅️ Режаларга", callback_data="menu:daily_plans")]
    ])

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
        
        # Kunlik rejalar statistika
        today_stats = daily_plans.get_all_plans_today()
        daily_plans_count = sum(len(plans) for plans in today_stats.values())
        daily_completed = 0
        for user_plans in today_stats.values():
            daily_completed += len([p for p in user_plans if p.get('completed', False)])
        
        # Bugungi muddati kelgan rejalar
        today_due_plans = daily_plans.get_today_plans_with_due_date()
        
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
        lines.append(f"\n📅 *Кунлик иш режалари:*")
        lines.append(f"  • Бугун қўшилган: {daily_plans_count} та")
        lines.append(f"  • Бажарилган: {daily_completed} та")
        lines.append(f"  • Бугун муддати: {len(today_due_plans)} та")
        
        return "\n".join(lines)
        
    except Exception as e:
        print(f"full_report xatolik: {e}")
        return "⚠️ Маълумотлар юкланмоқда..."
    finally:
        conn.close()

# =========================
# DAILY PLANS HANDLERS
# =========================

# =========================
# DAILY PLANS HANDLERS (ROLLAR BILAN)
# =========================

async def daily_plans_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Kunlik rejalar menyusi"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    # Admin uchun maxsus menyu
    if user_role == 'admin':
        text = "📅 *Кунлик иш режалари (Админ режими)*\n\nСиз админсиз. Барча фойдаланувчиларнинг режаларини кўра оласиз.\n\n*Муддат қўшиш учун:*\nРежа матнидан кейин муддатни қўшинг:\n\n`Режа матни | муддат (YYYY-MM-DD)`\n\n*Мисол:*\n`Ҳужжат тайёрлаш | 2024-01-20`"
        
        keyboard = [
            [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
            [InlineKeyboardButton("📋 Барча режалар", callback_data="daily:all_plans:0")],  # :0 sahifa raqami
            [InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")],
            [InlineKeyboardButton("📅 Келажакдаги режалар", callback_data="daily:upcoming")],
            [InlineKeyboardButton("📊 Бугунги статистика", callback_data="daily:stats")],
            [InlineKeyboardButton("⏰ Бугун муддати", callback_data="daily:today_due")],
            [InlineKeyboardButton("🧹 Менинг режаларимни тозалаш", callback_data="daily:clear")],
            [InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")]
        ]
    else:
        text = "📅 *Кунлик иш режалари*\n\nБу бўлимда фақат ўзингизнинг кунлик иш режаларингизни бошқаришингиз мумкин.\n\n*Муддат қўшиш учун:*\nРежа матнидан кейин муддатни қўшинг:\n\n`Режа матни | муддат (YYYY-MM-DD)`\n\n*Мисол:*\n`Ҳужжат тайёрлаш | 2024-01-20`"
        
        keyboard = [
            [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
            [InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")],
            [InlineKeyboardButton("📅 Келажакдаги режаларим", callback_data="daily:upcoming")],
            [InlineKeyboardButton("📊 Бугунги статистика", callback_data="daily:stats")],
            [InlineKeyboardButton("⏰ Бугун муддати", callback_data="daily:today_due")],
            [InlineKeyboardButton("🧹 Режаларимни тозалаш", callback_data="daily:clear")],
            [InlineKeyboardButton("⬅️ Орқага", callback_data="back:main")]
        ]
    
    await q.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_my_plans_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mening rejalarim"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Admin uchun barcha rejalar, oddiy foydalanuvchi uchun faqat o'zi kiritganlari
    if user_role == 'admin':
        plans = daily_plans.get_all_plans_for_admin(today)
        title = f"📋 *{today} кун учун барча иш режалари*\n"
    else:
        plans = daily_plans.get_user_plans(user_id, today)
        title = f"📋 *{today} кун учун менинг иш режаларим*\n"
    
    total = len(plans)
    completed = len([p for p in plans if p['completed']])
    
    lines = [
        title,
        f"📊 Статистика: {completed}/{total} та бажарилган ({int(completed/total*100 if total > 0 else 0)}%)",
        f""
    ]
    
    if not plans:
        lines.append("📭 Ҳозирча режалар мавжуд эмас")
    else:
        for plan in plans[:15]:  # Faqat birinchi 15 tasi
            status = "✅" if plan['completed'] else "🟡"
            created_time = plan['created_at'].split()[1][:5] if 'created_at' in plan else "N/A"
            due_info = f" | ⏰ {plan['due_date']}" if plan.get('due_date') else ""
            
            # Agar admin bo'lsa, kim kiritganligini ko'rsatish
            owner_info = ""
            if user_role == 'admin' and 'owner_user_id' in plan:
                owner_id = plan['owner_user_id']
                try:
                    from telegram import Chat
                    chat = await ctx.bot.get_chat(owner_id)
                    owner_name = chat.first_name or f"User {owner_id}"
                    owner_info = f"\n   👤 {owner_name}"
                except:
                    owner_info = f"\n   👤 User {owner_id}"
            
            lines.append(
                f"{status} *{plan['id']}. {plan['text']}*\n"
                f"   ⏰ {created_time}{due_info} | "
                f"{'✅ Бажарилган' if plan['completed'] else '⏳ Кутмоқда'}"
                f"{owner_info}"
            )
    
    keyboard = []
    
    # Har bir reja uchun amallar tugmasi
    if plans:
        for plan in plans[:10]:  # Faqat birinchi 10 tasi
            due_mark = "⏰ " if plan.get('due_date') else ""
            plan_date = today
            
            # Admin har qanday rejani boshqarishi mumkin
            if user_role == 'admin':
                owner_id = plan.get('owner_user_id', user_id)
                callback_data = f"daily:view:{plan_date}:{plan['id']}:{owner_id}"
            else:
                callback_data = f"daily:view:{plan_date}:{plan['id']}"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{'✅' if plan['completed'] else '🟡'} {plan['id']}. {due_mark}{plan['text'][:15]}...",
                    callback_data=callback_data
                )
            ])
    
    keyboard.append([InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")])
    
    if user_role == 'admin':
        keyboard.append([InlineKeyboardButton("📅 Келажакдагилар", callback_data="daily:upcoming")])
    else:
        keyboard.append([InlineKeyboardButton("📅 Келажакдаги режаларим", callback_data="daily:upcoming")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")])
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
async def daily_all_plans_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin uchun barcha rejalar (pagination bilan)"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    if user_role != 'admin':
        await q.answer("❌ Фақат админлар учун", show_alert=True)
        await daily_plans_cb(update, ctx)
        return
    
    data_parts = q.data.split(":")
    page = int(data_parts[2]) if len(data_parts) > 2 else 0
    
    today = datetime.now().strftime('%Y-%m-%d')
    all_plans = daily_plans.get_all_plans_for_admin(today)
    
    total = len(all_plans)
    offset = page * PAGE_SIZE
    
    lines = [
        f"📋 *{today} кун учун барча иш режалари*\n",
        f"Жами: {total} та режа",
        f"Саҳифа {page + 1}/{(total + PAGE_SIZE - 1) // PAGE_SIZE}\n"
    ]
    
    if not all_plans:
        lines.append("📭 Ҳозирча режалар мавжуд эмас")
    else:
        for i in range(offset, min(offset + PAGE_SIZE, total)):
            plan = all_plans[i]
            status = "✅" if plan['completed'] else "🟡"
            created_time = plan['created_at'].split()[1][:5] if 'created_at' in plan else "N/A"
            due_info = f" | ⏰ {plan['due_date']}" if plan.get('due_date') else ""
            
            # Kim kiritganligini aniqlash
            owner_id = plan.get('owner_user_id', user_id)
            try:
                from telegram import Chat
                chat = await ctx.bot.get_chat(owner_id)
                owner_name = chat.first_name or f"User {owner_id}"
            except:
                owner_name = f"User {owner_id}"
            
            # Reja matnini qisqartirish
            plan_text = plan['text']
            if len(plan_text) > 50:
                plan_text = plan_text[:47] + "..."
            
            lines.append(
                f"{status} *{i+1}. {plan_text}*\n"
                f"   👤 {owner_name}\n"
                f"   ⏰ {created_time}{due_info} | "
                f"{'✅ Бажарилган' if plan['completed'] else '⏳ Кутмоқда'}\n"
                f"   {'─' * 30}\n"
            )
    
    keyboard = []
    
    # Pager tugmalari
    if page > 0:
        keyboard.append([InlineKeyboardButton("◀️ Олдинги", callback_data=f"daily:all_plans:{page-1}")])
    if (page + 1) * PAGE_SIZE < total:
        if page > 0:
            keyboard[-1].append(InlineKeyboardButton("▶️ Кейинги", callback_data=f"daily:all_plans:{page+1}"))
        else:
            keyboard.append([InlineKeyboardButton("▶️ Кейинги", callback_data=f"daily:all_plans:{page+1}")])
    
    # Har bir reja uchun amallar tugmasi (faqat joriy sahifadagilar)
    current_page_plans = all_plans[offset:offset + PAGE_SIZE]
    if current_page_plans:
        for i, plan in enumerate(current_page_plans):
            due_mark = "⏰ " if plan.get('due_date') else ""
            owner_id = plan.get('owner_user_id', user_id)
            callback_data = f"daily:view:{today}:{plan['id']}:{owner_id}"
            
            # Tugma matnini qisqartirish
            btn_text = plan['text']
            if len(btn_text) > 25:
                btn_text = btn_text[:22] + "..."
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{'✅' if plan['completed'] else '🟡'} {plan['id']}. {due_mark}{btn_text}",
                    callback_data=callback_data
                )
            ])
    
    keyboard.append([InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")])
    keyboard.append([InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")])
    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")])
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
async def daily_upcoming_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Kelajakdagi rejalar"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    # Admin uchun barcha rejalar, oddiy foydalanuvchi uchun faqat o'z rejalari
    if user_role == 'admin':
        upcoming_plans = daily_plans.get_upcoming_plans(user_id, viewer_id=user_id)
        title = "📅 *Келажакдаги барча иш режалари*\n"
    else:
        upcoming_plans = daily_plans.get_upcoming_plans(user_id)
        title = "📅 *Келажакдаги иш режаларим*\n"
    
    lines = [
        title,
        f"Жами: {len(upcoming_plans)} та муддатли режа",
        f""
    ]
    
    if not upcoming_plans:
        lines.append("📭 Ҳозирча келажакдаги режалар мавжуд эмас")
    else:
        today = datetime.now().date()
        
        for item in upcoming_plans[:15]:  # Faqat birinchi 15 tasi
            plan_date = item['date']
            plan = item['plan']
            owner_id = item.get('owner_user_id', user_id)
            
            due_date = plan.get('due_date')
            if due_date:
                try:
                    due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                    days_left = (due_datetime - today).days
                    
                    if days_left < 0:
                        days_info = f"⛔ {abs(days_left)} кун ўтган"
                    elif days_left == 0:
                        days_info = "⚠️ Бугун муддати"
                    elif days_left <= 3:
                        days_info = f"⚠️ {days_left} кун қолди"
                    else:
                        days_info = f"📅 {days_left} кун қолди"
                except:
                    days_info = "📅 Муддатли"
            else:
                days_info = "📅 Муддатиз"
            
            # Agar admin bo'lsa, kim kiritganligini ko'rsatish
            owner_info = ""
            if user_role == 'admin':
                try:
                    from telegram import Chat
                    chat = await ctx.bot.get_chat(owner_id)
                    owner_name = chat.first_name or f"User {owner_id}"
                    owner_info = f"\n   👤 {owner_name}"
                except:
                    owner_info = f"\n   👤 User {owner_id}"
            
            lines.append(
                f"⏰ *{plan['id']}. {plan['text']}*\n"
                f"   📅 Муддат: {due_date or 'Муддатиз'}\n"
                f"   {days_info}\n"
                f"   🗓 Яратилган: {plan_date}"
                f"{owner_info}\n"
                f"   {'─' * 30}\n"
            )
    
    keyboard = [
        [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
    ]
    
    if user_role == 'admin':
        keyboard.append([InlineKeyboardButton("📋 Барча режалар", callback_data="daily:all_plans:0")])
        keyboard.append([InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")])
    else:
        keyboard.append([InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")])
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_view_plan_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejani ko'rish va boshqarish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    # Admin uchun owner_id ham keladi
    owner_id = int(data_parts[4]) if len(data_parts) > 4 else q.from_user.id
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    # Rejani topish
    plan = None
    if user_role == 'admin':
        # Admin har qanday rejani ko'ra oladi
        all_plans = daily_plans.get_all_plans_for_admin(plan_date)
        for p in all_plans:
            if p['id'] == plan_id:
                plan = p
                break
    else:
        # Oddiy foydalanuvchi faqat o'z rejasini
        plans = daily_plans.get_user_plans(user_id, plan_date)
        for p in plans:
            if p['id'] == plan_id:
                plan = p
                break
    
    if not plan:
        await q.answer("Режа топилмади ёки рухсатингиз йўқ", show_alert=True)
        if user_role == 'admin':
            await daily_all_plans_cb(update, ctx)
        else:
            await daily_my_plans_cb(update, ctx)
        return
    
    status = "✅ Бажарилган" if plan['completed'] else "⏳ Кутмоқда"
    completed_time = f"\n⏰ Бажарилган вақт: {plan['completed_at']}" if plan['completed'] and 'completed_at' in plan else ""
    
    due_info = f"\n⏰ *Муддат:* {plan['due_date']}" if plan.get('due_date') else ""
    
    # Kim kiritganligi
    owner_info = ""
    if user_role == 'admin' and 'owner_user_id' in plan:
        owner_id = plan['owner_user_id']
        try:
            from telegram import Chat
            chat = await ctx.bot.get_chat(owner_id)
            owner_name = chat.first_name or f"User {owner_id}"
            owner_info = f"\n👤 *Киритган:* {owner_name}"
        except:
            owner_info = f"\n👤 *Киритган:* User {owner_id}"
    
    # Muddati qolgan kunlar
    if plan.get('due_date') and not plan['completed']:
        try:
            today = datetime.now().date()
            due_datetime = datetime.strptime(plan['due_date'], '%Y-%m-%d').date()
            days_left = (due_datetime - today).days
            
            if days_left < 0:
                due_info += f"\n⛔ *{abs(days_left)} кун муддати ўтган*"
            elif days_left == 0:
                due_info += f"\n⚠️ *Бугун муддати!*"
            elif days_left <= 3:
                due_info += f"\n⚠️ *{days_left} кун қолди*"
            else:
                due_info += f"\n📅 *{days_left} кун қолди*"
        except:
            pass
    
    text = (
        f"📄 *Иш режаси №{plan['id']}*\n\n"
        f"📝 *Тавсиф:*\n{plan['text']}\n\n"
        f"📅 *Сана:* {plan_date}\n"
        f"⏰ *Яратилган:* {plan['created_at']}\n"
        f"{owner_info}"
        f"{due_info}\n"
        f"📊 *Ҳолати:* {status}"
        f"{completed_time}"
    )
    
    # Tugmalar
    if user_role == 'admin':
        # Admin uchun har qanday rejani boshqarish imkoniyati
        keyboard = [
            [
                InlineKeyboardButton("✅ Бажарилди", callback_data=f"daily:toggle:{plan_date}:{plan_id}:{owner_id}"),
                InlineKeyboardButton("❌ Ўчириш", callback_data=f"daily:delete:{plan_date}:{plan_id}:{owner_id}")
            ],
            [InlineKeyboardButton("⬅️ Режаларга", callback_data="daily:all_plans:0")]
        ]
    else:
        # Oddiy foydalanuvchi uchun faqat o'z rejasini boshqarish
        keyboard = [
            [
                InlineKeyboardButton("✅ Бажарилди", callback_data=f"daily:toggle:{plan_date}:{plan_id}"),
                InlineKeyboardButton("❌ Ўчириш", callback_data=f"daily:delete:{plan_date}:{plan_id}")
            ],
            [InlineKeyboardButton("⬅️ Режаларга", callback_data="menu:daily_plans")]
        ]
    
    await q.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_toggle_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reja holatini o'zgartirish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    # Admin uchun owner_id ham keladi
    owner_id = int(data_parts[4]) if len(data_parts) > 4 else q.from_user.id
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    success = daily_plans.toggle_plan(owner_id, plan_date, plan_id, viewer_id=user_id)
    
    if success:
        await q.answer("✅ Режа ҳолати ўзгартирилди", show_alert=True)
        await daily_view_plan_cb(update, ctx)
    else:
        await q.answer("❌ Хатолик юз берди ёки рухсатингиз йўқ", show_alert=True)

async def daily_delete_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejani o'chirish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    # Admin uchun owner_id ham keladi
    owner_id = int(data_parts[4]) if len(data_parts) > 4 else q.from_user.id
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    
    success = daily_plans.delete_plan(owner_id, plan_date, plan_id, viewer_id=user_id)
    
    if success:
        await q.answer("✅ Режа ўчирилди", show_alert=True)
        # Admin bo'lsa barcha rejalar sahifasiga, oddiy foydalanuvchi bo'lsa o'zi rejalari sahifasiga
        if user_role == 'admin':
            await daily_all_plans_cb(update, ctx)
        else:
            await daily_my_plans_cb(update, ctx)
    else:
        await q.answer("❌ Хатолик юз берди ёки рухсатингиз йўқ", show_alert=True)



async def daily_add_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Yangi reja qo'shish (muddat bilan)"""
    q = update.callback_query
    await q.answer()
    
    ctx.user_data['waiting_for_plan'] = True
    
    await q.edit_message_text(
        text="✏️ *Янги иш режасини киритинг:*\n\n*Формат:*\nРежа матни | муддат (YYYY-MM-DD)\n\n*Мисоллар:*\n• Ҳужжат тайёрлаш | 2024-01-20\n• Ҳамкор билан учрашув | 2024-01-22\n• Ҳисобот тақдимоти\n\n*Эътибор:* Агар муддат кўшмасангиз, фақат режа матнини киритинг.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Бекор қилиш", callback_data="menu:daily_plans")]]),
        parse_mode="Markdown"
    )

async def daily_my_plans_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mening rejalarim"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    today = datetime.now().strftime('%Y-%m-%d')
    
    plans = daily_plans.get_plans(user_id, today)
    total, completed = daily_plans.get_stats(user_id, today)
    
    lines = [
        f"📋 *{today} кун учун менинг иш режаларим*\n",
        f"📊 Статистика: {completed}/{total} та бажарилган ({int(completed/total*100 if total > 0 else 0)}%)",
        f""
    ]
    
    if not plans:
        lines.append("📭 Ҳозирча режалар мавжуд эмас")
    else:
        for plan in plans:
            status = "✅" if plan['completed'] else "🟡"
            created_time = plan['created_at'].split()[1][:5] if 'created_at' in plan else "N/A"
            due_info = f" | ⏰ {plan['due_date']}" if plan.get('due_date') else ""
            
            lines.append(
                f"{status} *{plan['id']}. {plan['text']}*\n"
                f"   ⏰ {created_time}{due_info} | "
                f"{'✅ Бажарилган' if plan['completed'] else '⏳ Кутмоқда'}"
            )
    
    keyboard = []
    
    # Har bir reja uchun amallar tugmasi
    if plans:
        for plan in plans[:10]:  # Faqat birinchi 10 tasi
            due_mark = "⏰ " if plan.get('due_date') else ""
            keyboard.append([
                InlineKeyboardButton(
                    f"{'✅' if plan['completed'] else '🟡'} {plan['id']}. {due_mark}{plan['text'][:15]}...",
                    callback_data=f"daily:view:{today}:{plan['id']}"
                )
            ])
    
    keyboard.append([InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")])
    keyboard.append([InlineKeyboardButton("📅 Келажакдагилар", callback_data="daily:upcoming")])
    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")])
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_upcoming_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Kelajakdagi rejalar"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    upcoming_plans = daily_plans.get_upcoming_plans(user_id)
    
    lines = [
        f"📅 *Келажакдаги иш режаларим*\n",
        f"Жами: {len(upcoming_plans)} та муддатли режа",
        f""
    ]
    
    if not upcoming_plans:
        lines.append("📭 Ҳозирча келажакдаги режалар мавжуд эмас")
    else:
        today = datetime.now().date()
        
        for item in upcoming_plans[:15]:  # Faqat birinchi 15 tasi
            plan_date = item['date']
            plan = item['plan']
            
            due_date = plan.get('due_date')
            if due_date:
                try:
                    due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                    days_left = (due_datetime - today).days
                    
                    if days_left < 0:
                        days_info = f"⛔ {abs(days_left)} кун ўтган"
                    elif days_left == 0:
                        days_info = "⚠️ Бугун муддати"
                    elif days_left <= 3:
                        days_info = f"⚠️ {days_left} кун қолди"
                    else:
                        days_info = f"📅 {days_left} кун қолди"
                except:
                    days_info = "📅 Муддатли"
            else:
                days_info = "📅 Муддатиз"
            
            lines.append(
                f"⏰ *{plan['id']}. {plan['text']}*\n"
                f"   📅 Муддат: {due_date or 'Муддатиз'}\n"
                f"   {days_info}\n"
                f"   🗓 Яратилган: {plan_date}\n"
                f"   {'─' * 30}\n"
            )
    
    keyboard = [
        [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
        [InlineKeyboardButton("📋 Бугунги режаларим", callback_data="daily:my_plans:0")],
        [InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")]
    ]
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_today_due_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Bugungi muddati kelgan rejalar"""
    q = update.callback_query
    await q.answer()
    
    today_due_plans = daily_plans.get_today_plans_with_due_date()
    
    lines = [
        f"⏰ *Бугун муддати келадиган иш режалари*\n",
        f"Жами: {len(today_due_plans)} та режа",
        f""
    ]
    
    if not today_due_plans:
        lines.append("✅ Бугун муддати келадиган режалар мавжуд эмас")
    else:
        for item in today_due_plans:
            user_id = item['user_id']
            plan_date = item['date']
            plan = item['plan']
            
            try:
                from telegram import Chat
                chat = await ctx.bot.get_chat(user_id)
                user_name = chat.first_name or f"Foydalanuvchi {user_id}"
            except:
                user_name = f"Foydalanuvchi {user_id}"
            
            lines.append(
                f"👤 *{user_name}*\n"
                f"   📝 {plan['text']}\n"
                f"   🗓 Яратилган: {plan_date}\n"
                f"   ⏰ Муддат: {plan.get('due_date')}\n"
                f"   {'─' * 30}\n"
            )
    
    keyboard = [
        [InlineKeyboardButton("➕ Янги режа қўшиш", callback_data="daily:add")],
        [InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")],
        [InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")]
    ]
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_view_plan_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejani ko'rish va boshqarish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    user_id = q.from_user.id
    plans = daily_plans.get_plans(user_id, plan_date)
    
    plan = None
    for p in plans:
        if p['id'] == plan_id:
            plan = p
            break
    
    if not plan:
        await q.answer("Режа топилмади", show_alert=True)
        await daily_my_plans_cb(update, ctx)
        return
    
    status = "✅ Бажарилган" if plan['completed'] else "⏳ Кутмоқда"
    completed_time = f"\n⏰ Бажарилган вақт: {plan['completed_at']}" if plan['completed'] and 'completed_at' in plan else ""
    
    due_info = f"\n⏰ *Муддат:* {plan['due_date']}" if plan.get('due_date') else ""
    
    # Muddati qolgan kunlar
    if plan.get('due_date') and not plan['completed']:
        try:
            today = datetime.now().date()
            due_datetime = datetime.strptime(plan['due_date'], '%Y-%m-%d').date()
            days_left = (due_datetime - today).days
            
            if days_left < 0:
                due_info += f"\n⛔ *{abs(days_left)} кун муддати ўтган*"
            elif days_left == 0:
                due_info += f"\n⚠️ *Бугун муддати!*"
            elif days_left <= 3:
                due_info += f"\n⚠️ *{days_left} кун қолди*"
            else:
                due_info += f"\n📅 *{days_left} кун қолди*"
        except:
            pass
    
    text = (
        f"📄 *Иш режаси №{plan['id']}*\n\n"
        f"📝 *Тавсиф:*\n{plan['text']}\n\n"
        f"📅 *Сана:* {plan_date}\n"
        f"⏰ *Яратилган:* {plan['created_at']}\n"
        f"{due_info}\n"
        f"📊 *Ҳолати:* {status}"
        f"{completed_time}"
    )
    
    await q.edit_message_text(
        text=text,
        reply_markup=plan_actions_menu(plan_date, plan_id),
        parse_mode="Markdown"
    )

async def daily_toggle_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reja holatini o'zgartirish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    success = daily_plans.toggle_plan(q.from_user.id, plan_date, plan_id)
    
    if success:
        await q.answer("✅ Режа ҳолати ўзгартирилди", show_alert=True)
        await daily_view_plan_cb(update, ctx)
    else:
        await q.answer("❌ Хатолик юз берди", show_alert=True)

async def daily_delete_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejani o'chirish"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    plan_date = data_parts[2]
    plan_id = int(data_parts[3])
    
    success = daily_plans.delete_plan(q.from_user.id, plan_date, plan_id)
    
    if success:
        await q.answer("✅ Режа ўчирилди", show_alert=True)
        await daily_my_plans_cb(update, ctx)
    else:
        await q.answer("❌ Хатолик юз берди", show_alert=True)

async def daily_stats_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Statistika"""
    q = update.callback_query
    await q.answer()
    
    user_id = q.from_user.id
    user_role = USERS.get(user_id, {}).get('role', 'user')
    today = datetime.now().strftime('%Y-%m-%d')
    
    lines = []
    
    if user_role == 'admin':
        # Admin uchun barcha rejalar statistikasi
        lines.append(f"📊 *Бугунги барча иш режалари статистикаси (Админ)*\n")
        
        # Admin uchun barcha rejalar
        all_plans_today = daily_plans.get_all_plans_for_admin(today)
        total = len(all_plans_today)
        completed = len([p for p in all_plans_today if p['completed']])
        
        # Har bir foydalanuvchi uchun statistikani alohida hisoblash
        user_stats = {}
        for plan in all_plans_today:
            owner_id = plan.get('owner_user_id', user_id)
            if owner_id not in user_stats:
                user_stats[owner_id] = {'total': 0, 'completed': 0}
            user_stats[owner_id]['total'] += 1
            if plan['completed']:
                user_stats[owner_id]['completed'] += 1
        
        lines.append(f"👤 *Админ:* {q.from_user.first_name}")
        lines.append(f"📅 *Бугунги кун ({today}):*")
        lines.append(f"  • Жами режалар: {total} та")
        lines.append(f"  • Бажарилган: {completed} та")
        lines.append(f"  • Бажарилмаган: {total - completed} та")
        lines.append(f"  • Бажариш фоизи: {int(completed/total*100 if total > 0 else 0)}%")
        lines.append(f"")
        
        # Har bir foydalanuvchi uchun statistikalar
        if user_stats:
            lines.append("👥 *Фойдаланувчилар бўйича:*")
            for owner_id, stats in user_stats.items():
                try:
                    from telegram import Chat
                    chat = await ctx.bot.get_chat(owner_id)
                    owner_name = chat.first_name or f"User {owner_id}"
                except:
                    owner_name = f"User {owner_id}"
                
                lines.append(f"  • {owner_name}: {stats['completed']}/{stats['total']} та ({int(stats['completed']/stats['total']*100 if stats['total'] > 0 else 0)}%)")
            lines.append("")
        
    else:
        # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
        lines.append(f"📊 *Кунлик иш режалари статистикаси*\n")
        
        total, completed = daily_plans.get_stats(user_id, today)
        lines.append(f"👤 *Фойдаланувчи:* {q.from_user.first_name}")
        lines.append(f"📅 *Бугунги кун ({today}):*")
        lines.append(f"  • Жами режалар: {total} та")
        lines.append(f"  • Бажарилган: {completed} та")
        lines.append(f"  • Бажарилмаган: {total - completed} та")
        lines.append(f"  • Бажариш фоизи: {int(completed/total*100 if total > 0 else 0)}%")
        lines.append(f"")
    
    # Oxirgi 7 kun statistikasi (faqat o'z rejalari uchun)
    week_stats = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        if user_role == 'admin':
            # Admin uchun har kundagi barcha rejalar
            daily_plans_list = daily_plans.get_all_plans_for_admin(date)
            t = len(daily_plans_list)
            c = len([p for p in daily_plans_list if p['completed']])
        else:
            # Oddiy foydalanuvchi uchun faqat o'z rejalari
            plans = daily_plans.get_user_plans(user_id, date)
            t = len(plans)
            c = len([p for p in plans if p['completed']])
        
        if t > 0:
            week_stats.append((date, t, c))
    
    if week_stats:
        lines.append("📈 *Охирги 7 кун статистикаси:*")
        for date, t, c in week_stats:
            lines.append(f"  • {date}: {c}/{t} та ({int(c/t*100 if t > 0 else 0)}%)")
        lines.append("")
    
    # Kelajakdagi rejalar
    upcoming_plans = daily_plans.get_upcoming_plans(user_id, viewer_id=user_id if user_role == 'admin' else None)
    
    overdue_plans = 0
    today_due_plans = 0
    future_due_plans = 0
    
    today_date = datetime.now().date()
    
    for item in upcoming_plans:
        plan = item['plan']
        due_date = plan.get('due_date')
        if due_date and not plan.get('completed', False):
            try:
                due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                days_left = (due_datetime - today_date).days
                
                if days_left < 0:
                    overdue_plans += 1
                elif days_left == 0:
                    today_due_plans += 1
                else:
                    future_due_plans += 1
            except:
                pass
    
    if user_role == 'admin':
        lines.append(f"⏰ *Барча муддатли режалар:*")
    else:
        lines.append(f"⏰ *Менинг муддатли режаларим:*")
    
    lines.extend([
        f"  • Муддати ўтган: {overdue_plans} та",
        f"  • Бугун муддати: {today_due_plans} та",
        f"  • Келажакдаги: {future_due_plans} та",
        f"  • Жами муддатли: {len(upcoming_plans)} та",
        f""
    ])
    
    # Umumiy statistika
    total_all = 0
    completed_all = 0
    
    if user_role == 'admin':
        # Admin uchun barcha rejalar
        for date_data in daily_plans.data.values():
            for user_plans in date_data.values():
                total_all += len(user_plans)
                completed_all += len([p for p in user_plans if p.get('completed', False)])
    else:
        # Oddiy foydalanuvchi uchun faqat o'zi kiritgan rejalar
        for date_data in daily_plans.data.values():
            if str(user_id) in date_data:
                user_plans = date_data[str(user_id)]
                total_all += len(user_plans)
                completed_all += len([p for p in user_plans if p.get('completed', False)])
    
    lines.extend([
        f"📊 *Умумий статистика:*",
        f"  • Жами режалар: {total_all} та",
        f"  • Бажарилган: {completed_all} та",
        f"  • Умумий бажариш фоизи: {int(completed_all/total_all*100 if total_all > 0 else 0)}%"
    ])
    
    # Bugungi muddati kelgan rejalar ro'yxati
    if user_role == 'admin':
        today_due_list = daily_plans.get_today_plans_with_due_date()
        if today_due_list:
            lines.append(f"\n⚠️ *Бугун муддати келадиган режалар:*")
            for item in today_due_list[:5]:  # Faqat birinchi 5 tasi
                owner_id = item['user_id']
                plan = item['plan']
                
                try:
                    from telegram import Chat
                    chat = await ctx.bot.get_chat(owner_id)
                    owner_name = chat.first_name or f"User {owner_id}"
                except:
                    owner_name = f"User {owner_id}"
                
                # Reja matnini qisqartirish
                plan_text = plan['text']
                if len(plan_text) > 30:
                    plan_text = plan_text[:27] + "..."
                
                lines.append(f"  • {owner_name}: {plan_text}")
    
    keyboard = []
    if user_role == 'admin':
        keyboard.append([InlineKeyboardButton("📋 Барча режалар", callback_data="daily:all_plans:0")])
    
    keyboard.append([InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")])
    keyboard.append([InlineKeyboardButton("⏰ Бугун муддати", callback_data="daily:today_due")])
    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")])
    
    await q.edit_message_text(
        text=safe_text(lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def daily_clear_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejalarni tozalash"""
    q = update.callback_query
    await q.answer()
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Ҳа, тозалаш", callback_data="daily:clear_confirm"),
            InlineKeyboardButton("❌ Ўққа", callback_data="menu:daily_plans")
        ]
    ])
    
    await q.edit_message_text(
        text="⚠️ *Диққат!*\n\nСиз ўзингизнинг *БУГУНГИ* иш режаларингизни тозаламоқчисиз. Бу амални бекор қилиб бўлмайди!\n\nТозалашни тасдиқлангми?",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

async def daily_clear_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rejalarni tozalashni tasdiqlash"""
    q = update.callback_query
    await q.answer()
    
    success = daily_plans.clear_plans(q.from_user.id)
    
    if success:
        text = "✅ Бугунги иш режаларингиз муваффақиятли тозаланди"
    else:
        text = "ℹ️ Тозалаш учун режалар мавжуд эмас"
    
    await q.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="menu:daily_plans")]]),
        parse_mode="Markdown"
    )

async def handle_text_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Matnli xabarlarni qayta ishlash (rejalar qo'shish uchun)"""
    if update.effective_user.id not in USERS:
        return
    
    if ctx.user_data.get('waiting_for_plan'):
        input_text = update.message.text.strip()
        
        if len(input_text) < 3:
            await update.message.reply_text(
                "❌ Режа матни жуда қисқа. Камда 3 та ҳарф киритинг.",
                parse_mode="Markdown"
            )
            return
        
        # Reja va muddatni ajratish
        plan_text = input_text
        due_date = None
        
        if '|' in input_text:
            parts = input_text.split('|', 1)
            if len(parts) == 2:
                plan_text = parts[0].strip()
                due_date_str = parts[1].strip()
                
                # Sanani formatlash
                try:
                    # Har xil formatlarni qabul qilish
                    date_formats = ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y']
                    parsed_date = None
                    
                    for fmt_str in date_formats:
                        try:
                            parsed_date = datetime.strptime(due_date_str, fmt_str)
                            break
                        except:
                            continue
                    
                    if parsed_date:
                        due_date = parsed_date.strftime('%Y-%m-%d')
                    else:
                        await update.message.reply_text(
                            "❌ Муддат нотоғри форматда. Тўғри формат: YYYY-MM-DD\nМисол: 2024-01-20",
                            parse_mode="Markdown"
                        )
                        return
                        
                except Exception as e:
                    await update.message.reply_text(
                        f"❌ Муддатни тушуниб бўлмади: {str(e)}",
                        parse_mode="Markdown"
                    )
                    return
        
        # Rejani qo'shish
        plan_id = daily_plans.add_plan(update.effective_user.id, plan_text, due_date)
        
        # Foydalanuvchi holatini tozalash
        ctx.user_data.pop('waiting_for_plan', None)
        
        # Javob
        response_text = (
            f"✅ *Янги иш режаси қўшилди!*\n\n"
            f"📝 *Режа:* {plan_text}\n"
            f"🔢 *ID:* {plan_id}\n"
            f"📅 *Сана:* {datetime.now().strftime('%Y-%m-%d')}\n"
        )
        
        if due_date:
            response_text += f"⏰ *Муддат:* {due_date}\n"
            
            # Muddati qancha qolganligini hisoblash
            try:
                today = datetime.now().date()
                due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                days_left = (due_datetime - today).days
                
                if days_left < 0:
                    response_text += f"⛔ *{abs(days_left)} кун муддати ўтган*\n"
                elif days_left == 0:
                    response_text += f"⚠️ *Бугун муддати!*\n"
                elif days_left <= 3:
                    response_text += f"⚠️ *{days_left} кун қолди*\n"
                else:
                    response_text += f"📅 *{days_left} кун қолди*\n"
            except:
                pass
        
        response_text += f"\nРежаларингизни кўриш учун /start буйруғини ишлатинг ёки 'Кунлик иш режалари' бўлимига ўтинг."
        
        await update.message.reply_text(
            response_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Менинг режаларим", callback_data="daily:my_plans:0")],
                [InlineKeyboardButton("📅 Келажакдагилар", callback_data="daily:upcoming")],
                [InlineKeyboardButton("🏠 Бош меню", callback_data="back:main")]
            ])
        )

# =========================
# MUDDAT ESKILATISH FUNKSIYALARI
# =========================

async def check_due_dates(context: ContextTypes.DEFAULT_TYPE):
    """Muddati kelgan rejalarni tekshirish va eslatma yuborish"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        today_due_plans = daily_plans.get_today_plans_with_due_date()
        
        if not today_due_plans:
            return
        
        for item in today_due_plans:
            user_id = item['user_id']
            plan_date = item['date']
            plan = item['plan']
            
            # Agar eslatma yuborilmagan bo'lsa
            if not plan.get('notified', False):
                try:
                    # Eslatma yuborish
                    message_text = (
                        f"⏰ *Иш режаси муддати!*\n\n"
                        f"📝 *Режа:* {plan['text']}\n"
                        f"🔢 *ID:* {plan['id']}\n"
                        f"📅 *Яратилган:* {plan_date}\n"
                        f"⏰ *Муддат:* {today}\n\n"
                        f"Илтимос, режани бажаринг ёда режа ҳолатини ўзгартиринг."
                    )
                    
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=message_text,
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("✅ Бажарилди", callback_data=f"daily:toggle:{plan_date}:{plan['id']}"),
                                InlineKeyboardButton("📋 Кўриш", callback_data=f"daily:view:{plan_date}:{plan['id']}")
                            ]
                        ])
                    )
                    
                    # Eslatma yuborilganligini belgilash
                    plan['notified'] = True
                    daily_plans._save_data()
                    
                except Exception as e:
                    print(f"Eslatma yuborishda xatolik {user_id}: {e}")
        
        # 3 kundan kam qolgan rejalarni eslatish
        for user_id in USERS:
            try:
                upcoming_plans = daily_plans.get_upcoming_plans(user_id)
                if not upcoming_plans:
                    continue
                
                urgent_plans = []
                today_date = datetime.now().date()
                
                for item in upcoming_plans:
                    due_date = item['plan'].get('due_date')
                    if due_date:
                        try:
                            due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                            days_left = (due_datetime - today_date).days
                            
                            if 1 <= days_left <= 3 and not item['plan'].get('completed', False):
                                urgent_plans.append(item)
                        except:
                            pass
                
                if urgent_plans:
                    lines = [
                        f"⚠️ *Тезкор муддатли иш режалари*\n",
                        f"Қолиш вақти 3 кундан кам:"
                    ]
                    
                    for item in urgent_plans[:5]:  # Faqat birinchi 5 tasi
                        plan_date = item['date']
                        plan = item['plan']
                        due_date = plan.get('due_date')
                        
                        try:
                            due_datetime = datetime.strptime(due_date, '%Y-%m-%d').date()
                            days_left = (due_datetime - today_date).days
                            
                            lines.append(
                                f"\n⏰ *{plan['id']}. {plan['text']}*\n"
                                f"   📅 Муддат: {due_date}\n"
                                f"   ⚠️ Қолган вақт: {days_left} кун"
                            )
                        except:
                            pass
                    
                    if len(lines) > 2:  # Faqat tezkor rejalar bo'lsa
                        try:
                            await context.bot.send_message(
                                chat_id=user_id,
                                text="\n".join(lines),
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📅 Келажакдаги режаларим", callback_data="daily:upcoming")],
                                    [InlineKeyboardButton("📋 Бугунги режаларим", callback_data="daily:my_plans:0")]
                                ])
                            )
                        except Exception as e:
                            print(f"Tegishli eslatmani yuborishda xatolik {user_id}: {e}")
                            
            except Exception as e:
                print(f"Tegishli rejalarni tekshirishda xatolik {user_id}: {e}")
                
    except Exception as e:
        print(f"check_due_dates xatolik: {e}")

# =========================
# QOLGAN KODLAR O'ZGARMADI (size_cb, corp_cb, dist_cb, va boshqalar)
# =========================
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

# Iltimos, yuqoridagi funksiyalarnи o'zgarmagan holda qoldiring

# Faqat daily plans bilan bog'liq funksiyalar o'zgartirildi

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
        
        # Kunlik rejalar menyusi
        if key == "daily_plans":
            await daily_plans_cb(update, ctx)
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

# =========================
# DAILY PLANS COMMAND HANDLERS
# =========================

async def daily_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Kunlik rejalar uchun callback handler"""
    q = update.callback_query
    await q.answer()
    
    data_parts = q.data.split(":")
    action = data_parts[1]
    
    if action == "add":
        await daily_add_cb(update, ctx)
    elif action == "my_plans":
        await daily_my_plans_cb(update, ctx)
    elif action == "all_plans":  # YANGI QO'SHILDI
        await daily_all_plans_cb(update, ctx)
    elif action == "upcoming":
        await daily_upcoming_cb(update, ctx)
    elif action == "view":
        await daily_view_plan_cb(update, ctx)
    elif action == "toggle":
        await daily_toggle_cb(update, ctx)
    elif action == "delete":
        await daily_delete_cb(update, ctx)
    elif action == "stats":
        await daily_stats_cb(update, ctx)
    elif action == "today_due":
        await daily_today_due_cb(update, ctx)
    elif action == "clear":
        await daily_clear_cb(update, ctx)
    elif action == "clear_confirm":
        await daily_clear_confirm_cb(update, ctx)

# Tumanlar ro'yxati - tartib muhim!
ALLOWED_DISTRICTS = [
    "Наманган шаҳри",
    "Давлатобод тумани",
    "Янги Наманган тумани",
    "Мингбулоқ тумани",
    "Косонсой тумани",
    "Наманган тумани",
    "Норин тумани",
    "Поп тумани",
    "Тўрақўрғон тумани",
    "Уйчи тумани",
    "Учқўрғон тумани",
    "Чортоқ тумани",
    "Чуст тумани",
    "Янгиқўрғон тумани"
]

# Jadvaldan tuman bo'yicha statistika olish
def generate_daily_report(df):
    if df.empty:
        return "Bugungi kun uchun ma'lumot kiritilmagan."

    # D ustuni tuman deb hisoblaymiz (4-ustun, indeks 3)
    df.columns = df.columns.str.strip()  # ustun nomlaridagi bo'shliqlarni tozalash
    district_col = df.columns[3]         # odatda 4-ustun

    stats = {}
    for dist in ALLOWED_DISTRICTS:
        count = len(df[df[district_col].str.strip() == dist])
        stats[dist] = count

    # Umumiy statistika
    total_tasks = len(df)
    active_districts = sum(1 for v in stats.values() if v > 0)
    
    # Hisobot matni
    lines = []
    lines.append(f"📅 Kunlik ishlar hisoboti – {df.attrs.get('date', 'sana ko‘rsatilmagan')}")
    lines.append(f"Jami kiritilgan vazifalar: {total_tasks} ta")
    lines.append(f"Faol tumanlar soni: {active_districts} ta\n")
    
    lines.append("Tumanlar bo'yicha holat (tartib o‘zgarmaydi):")
    lines.append("─" * 45)
    
    for dist in ALLOWED_DISTRICTS:
        cnt = stats[dist]
        if cnt > 0:
            lines.append(f"🏙 {dist:<18} | {cnt:3d} ta vazifa")
        else:
            lines.append(f"🏙 {dist:<18} | —")
    
    lines.append("─" * 45)
    
    # Oddiy tahlil
    max_dist = max(stats, key=stats.get)
    min_dist_active = min((d for d in ALLOWED_DISTRICTS if stats[d] > 0), key=lambda d: stats[d], default=None)
    
    if total_tasks > 0:
        lines.append("\nQisqa tahlil:")
        lines.append(f"• Eng ko'p vazifa kiritgan tuman: {max_dist} ({stats[max_dist]} ta)")
        if min_dist_active:
            lines.append(f"• Eng kam vazifa (lekin bor): {min_dist_active} ({stats[min_dist_active]} ta)")
        lines.append("• Bo'sh tumanlar — keyingi kun uchun e'tibor berish kerak!")
    
    return "\n".join(lines)

async def daily_works_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    df = get_daily_works()
    if df.empty:
        await query.edit_message_text("Hozircha kunlik ishlar jadvalida ma'lumot yo'q.")
        return
    
    date_str = df.attrs.get('date', 'sana ko‘rsatilmagan')
    
    # Tumanlar bo'yicha guruhlash
    district_col = df.columns[3]  # D ustuni
    stats = df[district_col].value_counts().to_dict()
    
    # Faqat ro'yxatdagi tumanlar va faol bo'lganlari
    active_districts = [d for d in ALLOWED_DISTRICTS if d in stats and stats[d] > 0]
    
    if not active_districts:
        text = f"📅 Kunlik ishlar hisoboti – {date_str}\n\nBugun hech qaysi tuman vazifa kiritmagan."
    else:
        lines = [f"📅 Kunlik ishlar hisoboti – {date_str}"]
        lines.append(f"Faol tumanlar soni: {len(active_districts)} ta (jami vazifalar: {len(df)} ta)")
        lines.append("─" * 50)
        
        buttons = []
        for dist in active_districts:
            count = stats[dist]
            lines.append(f"🏙 {dist:<20} — {count} ta vazifa")
            buttons.append(
                InlineKeyboardButton(
                    f"{dist} ({count})",
                    callback_data=f"daily_works:detail:{dist}"
                )
            )
        
        lines.append("─" * 50)
        lines.append("Qisqa tahlil:")
        max_d = max(stats, key=stats.get)
        lines.append(f"• Eng faol tuman: {max_d} ({stats[max_d]} ta)")
        
        text = "\n".join(lines)
    
    # Tugmalar (faqat faol tumanlar uchun)
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])
    keyboard.append([InlineKeyboardButton("🔙 Orqaga", callback_data="menu:main")])
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def daily_works_detail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # callback_data = daily_works:detail:Туман номи
    _, _, district = query.data.split(":", 2)
    
    df = get_daily_works()
    if df.empty:
        await query.edit_message_text("Ma'lumot yo'q.")
        return
    
    district_col = df.columns[3]      # D ustuni - tuman
    task_col = df.columns[1]          # B ustuni - vazifa nomi (sizning jadvalingizga qarab o'zgartiring)
    status_col = df.columns[2]        # C ustuni - bajarilish holati
    
    district_df = df[df[district_col].str.strip() == district].copy()
    
    if district_df.empty:
        text = f"{district} tumani uchun bugun vazifa topilmadi."
    else:
        lines = [
            f"📋 {district} tumani – Kunlik reja ({df.attrs.get('date', 'sana')})",
            f"Jami vazifalar: {len(district_df)} ta",
            "─" * 45,
        ]
        
        for i, row in district_df.iterrows():
            task = row[task_col][:60] + "..." if len(row[task_col]) > 60 else row[task_col]
            status = row[status_col] if pd.notna(row[status_col]) else "—"
            lines.append(f"{i+1:2d}. {task}")
            lines.append(f"   Holat: {status}\n")
        
        lines.append("─" * 45)
        lines.append("PDF shaklida saqlash uchun quyidagi tugmani bosing ↓")
        
        text = "\n".join(lines)
    
    keyboard = [
        [InlineKeyboardButton("📄 PDF yuklab olish", callback_data=f"daily_works:pdf:{district}")],
        [InlineKeyboardButton("🔙 Hisobotga qaytish", callback_data="daily_works:report")],
        [InlineKeyboardButton("🏠 Asosiy menyuga", callback_data="menu:main")]
    ]
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
# requirements.txt ga qo'shing:
# reportlab

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from io import BytesIO

async def daily_works_pdf_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    _, _, district = query.data.split(":", 2)
    
    df = get_daily_works()
    district_df = df[df[df.columns[3]].str.strip() == district]
    
    if district_df.empty:
        await query.answer("Ma'lumot yo'q", show_alert=True)
        return
    
    # PDF yaratish
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 80, f"{district} tumani – Kunlik ish rejasi")
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 110, f"Sana: {df.attrs.get('date', '—')}")
    
    y = height - 150
    for i, row in district_df.iterrows():
        task = str(row[1])[:80] + "..." if len(str(row[1])) > 80 else str(row[1])
        status = str(row[2]) if pd.notna(row[2]) else "—"
        c.drawString(50, y, f"{i+1}. {task}")
        c.drawString(70, y - 15, f"Holat: {status}")
        y -= 40
        if y < 100:
            c.showPage()
            y = height - 80
    
    c.save()
    buffer.seek(0)
    
    # PDF ni yuborish
    await context.bot.send_document(
        chat_id=query.message.chat_id,
        document=buffer,
        filename=f"kunlik_reja_{district}_{df.attrs.get('date', 'sana')}.pdf",
        caption=f"{district} tumani uchun kunlik reja"
    )
    
    await query.answer("PDF yuborildi!", show_alert=True)

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
# DAILY REPORT FUNCTION
# =========================

async def daily_daily_report(context: ContextTypes.DEFAULT_TYPE):
    """Kunlik ish rejalari hisoboti"""
    try:
        # Bugungi kunlik rejalar statistikasi
        today_stats = daily_plans.get_all_plans_today()
        
        if not today_stats:
            return
        
        total_plans = 0
        completed_plans = 0
        users_with_plans = []
        
        for user_id_str, plans in today_stats.items():
            user_id = int(user_id_str)
            if user_id in USERS:
                total = len(plans)
                completed = len([p for p in plans if p.get('completed', False)])
                total_plans += total
                completed_plans += completed
                users_with_plans.append((user_id, total, completed))
        
        if total_plans == 0:
            return
        
        # Bugungi muddati kelgan rejalar
        today_due_plans = daily_plans.get_today_plans_with_due_date()
        
        # Hisobot matnini tayyorlash
        today_date = datetime.now().strftime('%d.%m.%Y')
        lines = [
            f"📅 *Кунлик иш режалари ҳисоботи*",
            f"*Сана:* {today_date}",
            f"",
            f"📊 *Умумий статистика:*",
            f"• Жами режалар: *{total_plans} та*",
            f"• Бажарилган: *{completed_plans} та*",
            f"• Бажарилмаган: *{total_plans - completed_plans} та*",
            f"• Бажариш фоизи: *{int(completed_plans/total_plans*100 if total_plans > 0 else 0)}%*",
            f"• Бугун муддати: *{len(today_due_plans)} та*",
            f"",
            f"👥 *Фойдаланувчилар бўйича:*"
        ]
        
        for user_id, total, completed in users_with_plans[:10]:  # Faqat birinchi 10 tasi
            try:
                from telegram import Chat
                chat = await context.bot.get_chat(user_id)
                user_name = chat.first_name or f"User {user_id}"
                lines.append(f"• {user_name}: {completed}/{total} та ({int(completed/total*100 if total > 0 else 0)}%)")
            except:
                lines.append(f"• User {user_id}: {completed}/{total} та")
        
        text = safe_text(lines)
        
        # Adminlarga yuborish
        for user_id in USERS:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"❌ Kunlik hisobotni {user_id} ga yuborib bo'lmadi: {e}")
                
    except Exception as e:
        print(f"daily_daily_report xatolik: {e}")

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
    app.add_handler(CallbackQueryHandler(menu_cb, pattern="^menu:"))
    
    # Loyihalar bilan bog'liq handlerlar (o'zgarmagan)
    app.add_handler(CallbackQueryHandler(corp_cb, pattern="^corp:"))
    app.add_handler(CallbackQueryHandler(dist_cb, pattern="^dist:"))
    app.add_handler(CallbackQueryHandler(corpdist_cb, pattern="^corpdist:"))
    app.add_handler(CallbackQueryHandler(back_cb, pattern="^back:"))
    app.add_handler(CallbackQueryHandler(size_cb, pattern="^size:"))
    app.add_handler(CallbackQueryHandler(size_dist_cb, pattern="^sizeDist:"))
    app.add_handler(CallbackQueryHandler(problem_district_detail_cb, pattern="^prob_dist:"))
    
    # Kunlik rejalar uchun yangi handlerlar
    app.add_handler(CallbackQueryHandler(daily_cb, pattern="^daily:"))
#    app.add_handler(CallbackQueryHandler(daily_works_cb, pattern="^daily_works:"))
    app.add_handler(CallbackQueryHandler(daily_works_cb, pattern="^daily_works:report$"))
    app.add_handler(CallbackQueryHandler(daily_works_detail_cb, pattern="^daily_works:detail:"))
    app.add_handler(CallbackQueryHandler(daily_works_pdf_cb, pattern="^daily_works:pdf:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # ===== BACKGROUND TASKS =====
    
    async def schedule_reports():
        """ Kunlik hisobotlarni jo'natish """
        while True:
            now = datetime.now(pytz.timezone('Asia/Tashkent'))
            current_time = now.strftime('%H:%M')
            
            # Kunlik muammoli loyihalar hisoboti (soat 17:00)
            if current_time == "17:00":
                await daily_problem_report(app)
                await asyncio.sleep(60)  # 1 minut kutish
            
            # Kunlik ish rejalari hisoboti (soat 19:00)
            elif current_time == "19:00":
                await daily_daily_report(app)
                await asyncio.sleep(60)  # 1 minut kutish
            
            # Muddati kelgan rejalarni tekshirish (har soat)
            elif current_time.endswith(":00"):
                await check_due_dates(app)
                await asyncio.sleep(60)  # 1 minut kutish
            
            else:
                # Har minut tekshirish
                await asyncio.sleep(60)
    
    # Background task
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(schedule_reports())
    except:
        print("⚠️ Report scheduler ishlamadi, lekin bot ishlaydi")

    print("\n" + "="*50)
    print("🤖 BOT ISHGA TUSHDI! (SQL + Daily Plans with Due Dates)")
    print("="*50)
    print("📊 Google Sheets har 5 minutda SQLite bazaga yangilanadi")
    print(f"💾 Database fayli: {DB_FILE}")
    print(f"📅 Kunlik rejalar fayli: {DAILY_PLANS_FILE}")
    print("⏰ Kunlik hisobotlar:")
    print("  • Muammoli loyihalar: har kuni soat 17:00 da")
    print("  • Ish rejalari: har kuni soat 19:00 da")
    print("  • Muddati eslatmalar: har soat")
    print("📝 Yangi funksiyalar:")
    print("  • Ish rejalariga muddat qo'shish (format: текст | YYYY-MM-DD)")
    print("  • Muddati kelganda eslatma yuborish")
    print("  • Kelajakdagi rejalarni ko'rish")
    print("  • Bugungi muddati kelgan rejalar")
    print("✅ /start buyrug'ini tekshiring\n")
    
    app.run_polling()

if __name__ == "__main__":
    main()