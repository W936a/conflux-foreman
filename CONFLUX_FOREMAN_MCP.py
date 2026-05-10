#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║           CONFLUX FOREMAN MCP SERVER v1.1                        ║
║           CONFLUX SYSTEMS (PTY) LTD — CAPE TOWN, SOUTH AFRICA    ║
╠══════════════════════════════════════════════════════════════════╣
║  PROPRIETARY AND CONFIDENTIAL                                     ║
║  © 2026 CONFLUX SYSTEMS (PTY) LTD. ALL RIGHTS RESERVED.          ║
╠══════════════════════════════════════════════════════════════════╣
║  FIXES APPLIED:                                                   ║
║  ✅ Bug 1: log.warning moved to lifespan startup                  ║
║  ✅ Bug 2: Fixed JSON schemas for create_invoice/create_quote     ║
║  ✅ Bug 3: delete_all_data now cascades to items and payments     ║
║  ✅ Added client_id to DeleteClientRequest                        ║
║  ✅ Added invoice_id to UpdateInvoiceStatusRequest                ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL DEPENDENCIES:
  pip install fastapi uvicorn httpx PyJWT pydantic redis --break-system-packages
  pip install reportlab  # Optional: for PDF generation

RUN:
  # HTTP mode (production)
  export FOREMAN_JWT_SECRET=your_secret
  python CONFLUX_FOREMAN_MCP.py

  # stdio mode (Claude Desktop)
  export FOREMAN_DEFAULT_USER=user_id
  python CONFLUX_FOREMAN_MCP.py --stdio
"""

import asyncio
import hmac
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import httpx
import jwt
from fastapi import FastAPI, HTTPException, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
import uvicorn

# Optional Redis
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Optional PDF generation — log warning moved to startup
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    # Warning will be logged in lifespan startup

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════

class Config:
    SERVER_NAME        = "conflux-foreman"
    SERVER_VERSION     = "1.1.0"
    DISPLAY_NAME       = "FOREMAN"
    VENDOR             = "CONFLUX SYSTEMS (PTY) LTD"
    PROTOCOL_VERSION   = "2024-11-05"

    # Auth — STRICT: no fallback
    JWT_SECRET         = os.getenv("FOREMAN_JWT_SECRET")
    API_KEY            = os.getenv("FOREMAN_API_KEY")

    # Database
    DB_PATH            = os.getenv("FOREMAN_DB_PATH", "./foreman.db")

    # Redis (optional)
    REDIS_URL          = os.getenv("REDIS_URL", "")

    # Server
    PORT               = int(os.getenv("PORT", "8084"))
    HOST               = os.getenv("HOST", "0.0.0.0")

    # Rate limits
    RATE_LIMIT_DEFAULT = int(os.getenv("RATE_LIMIT_DEFAULT", "60"))
    RATE_LIMIT_WRITE   = int(os.getenv("RATE_LIMIT_WRITE", "30"))

    # Business settings
    VAT_RATE           = float(os.getenv("VAT_RATE", "0.15"))
    DEFAULT_CURRENCY   = os.getenv("DEFAULT_CURRENCY", "ZAR")
    COMPANY_NAME       = os.getenv("COMPANY_NAME", "CONFLUX SYSTEMS (PTY) LTD")
    COMPANY_REG_NUMBER = os.getenv("COMPANY_REG_NUMBER", "2026/123456/07")
    COMPANY_VAT_NUMBER = os.getenv("COMPANY_VAT_NUMBER", "4567890123")

    @classmethod
    def validate(cls):
        if not cls.JWT_SECRET and not cls.API_KEY:
            raise ValueError("FOREMAN_JWT_SECRET or FOREMAN_API_KEY is required")

config = Config()

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FOREMAN] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("conflux.foreman")

# ══════════════════════════════════════════════════════════════
# RATE LIMITER
# ══════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self):
        self.redis = None
        self._local_windows: Dict[str, deque] = defaultdict(deque)

    async def connect(self):
        if config.REDIS_URL and REDIS_AVAILABLE:
            try:
                self.redis = await redis.from_url(config.REDIS_URL, decode_responses=True)
                log.info("Redis rate limiter connected")
                return
            except Exception as e:
                log.warning(f"Redis connection failed: {e}")
        log.info("Using in-memory rate limiter")

    async def check(self, key: str, limit: int, window_seconds: int = 60) -> Tuple[bool, int]:
        if self.redis:
            now = time.time()
            window_start = now - window_seconds
            pipe = self.redis.pipeline()
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zcard(key)
            pipe.zadd(key, {str(now): now})
            pipe.expire(key, window_seconds)
            results = await pipe.execute()
            count = results[1]
            if count >= limit:
                return False, 0
            return True, limit - count - 1
        else:
            now = time.time()
            window = self._local_windows[key]
            while window and window[0] < now - window_seconds:
                window.popleft()
            if len(window) >= limit:
                return False, 0
            window.append(now)
            return True, limit - len(window)

    def get_key(self, client_id: str, tool: str) -> str:
        return f"ratelimit:foreman:{client_id}:{tool}"

rate_limiter = RateLimiter()

# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════

class ForemanDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        cursor = conn.cursor()

        # Clients table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                tax_number TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Invoices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                invoice_number TEXT UNIQUE,
                issue_date TEXT NOT NULL,
                due_date TEXT NOT NULL,
                subtotal REAL NOT NULL,
                vat REAL NOT NULL,
                total REAL NOT NULL,
                status TEXT DEFAULT 'draft',
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)

        # Invoice items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS invoice_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id TEXT NOT NULL,
                description TEXT NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                amount REAL NOT NULL,
                FOREIGN KEY (invoice_id) REFERENCES invoices(id)
            )
        """)

        # Payments table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                amount REAL NOT NULL,
                payment_date TEXT NOT NULL,
                payment_method TEXT,
                reference TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_id) REFERENCES invoices(id)
            )
        """)

        # Quotes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                quote_number TEXT UNIQUE,
                issue_date TEXT NOT NULL,
                valid_until TEXT NOT NULL,
                subtotal REAL NOT NULL,
                vat REAL NOT NULL,
                total REAL NOT NULL,
                status TEXT DEFAULT 'draft',
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)

        # Quote items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quote_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT NOT NULL,
                description TEXT NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                amount REAL NOT NULL,
                FOREIGN KEY (quote_id) REFERENCES quotes(id)
            )
        """)

        conn.commit()
        conn.close()
        log.info(f"Database initialized at {self.db_path}")

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()

    # ==================== CLIENT METHODS ====================
    def add_client(self, user_id: str, name: str, email: str = None,
                   phone: str = None, address: str = None, tax_number: str = None) -> Dict:
        client_id = str(uuid.uuid4())[:8]
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO clients (id, user_id, name, email, phone, address, tax_number)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (client_id, user_id, name, email, phone, address, tax_number))
            conn.commit()
        return {"id": client_id, "name": name, "message": "Client added successfully"}

    def get_clients(self, user_id: str) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, email, phone, address, tax_number, created_at
                FROM clients WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
            rows = cursor.fetchall()
        return [{
            "id": r[0], "name": r[1], "email": r[2], "phone": r[3],
            "address": r[4], "tax_number": r[5], "created_at": r[6]
        } for r in rows]

    def get_client(self, user_id: str, client_id: str) -> Optional[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, email, phone, address, tax_number, created_at
                FROM clients WHERE user_id = ? AND id = ?
            """, (user_id, client_id))
            row = cursor.fetchone()
        if row:
            return {
                "id": row[0], "name": row[1], "email": row[2], "phone": row[3],
                "address": row[4], "tax_number": row[5], "created_at": row[6]
            }
        return None

    def update_client(self, user_id: str, client_id: str, **kwargs) -> bool:
        allowed = ["name", "email", "phone", "address", "tax_number"]
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if not updates:
            return False
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values()) + [user_id, client_id]
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE clients SET {set_clause} WHERE user_id = ? AND id = ?", values)
            conn.commit()
            return cursor.rowcount > 0

    def delete_client(self, user_id: str, client_id: str, confirm: bool = False) -> Dict:
        if not confirm:
            return {"error": "Delete requires confirm=True"}
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM clients WHERE user_id = ? AND id = ?", (user_id, client_id))
            deleted = cursor.rowcount
            conn.commit()
        return {"deleted": deleted, "message": f"Deleted {deleted} client"}

    # ==================== INVOICE METHODS ====================
    def _generate_invoice_number(self) -> str:
        """Generate unique invoice number: INV-YYYYMMDD-XXXX"""
        date_str = datetime.now().strftime("%Y%m%d")
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM invoices WHERE invoice_number LIKE ?", (f"INV-{date_str}-%",))
            count = cursor.fetchone()[0] + 1
        return f"INV-{date_str}-{count:04d}"

    def create_invoice(self, user_id: str, client_id: str, items: List[Dict],
                       issue_date: str = None, due_date: str = None,
                       notes: str = None, status: str = "draft") -> Dict:
        if not issue_date:
            issue_date = datetime.now().isoformat()
        if not due_date:
            due_date = (datetime.now() + timedelta(days=30)).isoformat()

        invoice_id = str(uuid.uuid4())[:8]
        invoice_number = self._generate_invoice_number()

        subtotal = sum(item["quantity"] * item["unit_price"] for item in items)
        vat = subtotal * config.VAT_RATE
        total = subtotal + vat

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO invoices (id, user_id, client_id, invoice_number, issue_date, due_date,
                                     subtotal, vat, total, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (invoice_id, user_id, client_id, invoice_number, issue_date, due_date,
                  subtotal, vat, total, status, notes))

            for item in items:
                amount = item["quantity"] * item["unit_price"]
                cursor.execute("""
                    INSERT INTO invoice_items (invoice_id, description, quantity, unit_price, amount)
                    VALUES (?, ?, ?, ?, ?)
                """, (invoice_id, item["description"], item["quantity"], item["unit_price"], amount))

            conn.commit()

        return {
            "id": invoice_id,
            "invoice_number": invoice_number,
            "subtotal": subtotal,
            "vat": vat,
            "total": total,
            "status": status,
            "message": f"Invoice {invoice_number} created"
        }

    def get_invoices(self, user_id: str, status: str = None, limit: int = 50) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            query = """
                SELECT i.id, i.invoice_number, c.name as client_name, i.issue_date, i.due_date,
                       i.subtotal, i.vat, i.total, i.status, i.created_at
                FROM invoices i
                JOIN clients c ON i.client_id = c.id
                WHERE i.user_id = ?
            """
            params = [user_id]
            if status:
                query += " AND i.status = ?"
                params.append(status)
            query += " ORDER BY i.created_at DESC LIMIT ?"
            params.append(limit)
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [{
            "id": r[0], "invoice_number": r[1], "client_name": r[2],
            "issue_date": r[3], "due_date": r[4], "subtotal": r[5],
            "vat": r[6], "total": r[7], "status": r[8], "created_at": r[9]
        } for r in rows]

    def get_invoice(self, user_id: str, invoice_id: str) -> Optional[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT i.id, i.invoice_number, i.client_id, c.name as client_name,
                       c.email, c.phone, c.address, c.tax_number,
                       i.issue_date, i.due_date, i.subtotal, i.vat, i.total,
                       i.status, i.notes, i.created_at
                FROM invoices i
                JOIN clients c ON i.client_id = c.id
                WHERE i.user_id = ? AND i.id = ?
            """, (user_id, invoice_id))
            row = cursor.fetchone()
            if not row:
                return None

            cursor.execute("""
                SELECT description, quantity, unit_price, amount
                FROM invoice_items WHERE invoice_id = ?
            """, (invoice_id,))
            items = [{"description": r[0], "quantity": r[1], "unit_price": r[2], "amount": r[3]} for r in cursor.fetchall()]

        return {
            "id": row[0], "invoice_number": row[1], "client_id": row[2],
            "client_name": row[3], "client_email": row[4], "client_phone": row[5],
            "client_address": row[6], "client_tax_number": row[7],
            "issue_date": row[8], "due_date": row[9], "subtotal": row[10],
            "vat": row[11], "total": row[12], "status": row[13],
            "notes": row[14], "created_at": row[15], "items": items
        }

    def update_invoice_status(self, user_id: str, invoice_id: str, status: str) -> bool:
        allowed = ["draft", "sent", "paid", "overdue", "cancelled"]
        if status not in allowed:
            return False
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE invoices SET status = ? WHERE user_id = ? AND id = ?",
                          (status, user_id, invoice_id))
            conn.commit()
            return cursor.rowcount > 0

    def add_payment(self, user_id: str, invoice_id: str, amount: float,
                    payment_date: str = None, payment_method: str = None,
                    reference: str = None) -> Dict:
        payment_id = str(uuid.uuid4())[:8]
        if not payment_date:
            payment_date = datetime.now().isoformat()

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO payments (id, invoice_id, amount, payment_date, payment_method, reference)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (payment_id, invoice_id, amount, payment_date, payment_method, reference))

            # Update invoice status if fully paid
            cursor.execute("SELECT total FROM invoices WHERE user_id = ? AND id = ?", (user_id, invoice_id))
            row = cursor.fetchone()
            if row:
                total = row[0]
                cursor.execute("SELECT SUM(amount) FROM payments WHERE invoice_id = ?", (invoice_id,))
                paid = cursor.fetchone()[0] or 0
                if paid >= total:
                    cursor.execute("UPDATE invoices SET status = 'paid' WHERE user_id = ? AND id = ?",
                                  (user_id, invoice_id))

            conn.commit()

        return {"payment_id": payment_id, "message": f"Payment of R{amount:.2f} recorded"}

    def get_payments(self, user_id: str, invoice_id: str = None) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            if invoice_id:
                cursor.execute("""
                    SELECT id, amount, payment_date, payment_method, reference, created_at
                    FROM payments WHERE invoice_id = ?
                    ORDER BY payment_date DESC
                """, (invoice_id,))
            else:
                cursor.execute("""
                    SELECT p.id, p.amount, p.payment_date, p.payment_method, p.reference,
                           i.invoice_number, p.created_at
                    FROM payments p
                    JOIN invoices i ON p.invoice_id = i.id
                    WHERE i.user_id = ?
                    ORDER BY p.payment_date DESC
                """, (user_id,))
            rows = cursor.fetchall()
        return [{"id": r[0], "amount": r[1], "payment_date": r[2], "payment_method": r[3],
                 "reference": r[4], "invoice_number": r[5] if len(r) > 5 else None, "created_at": r[-1]}
                for r in rows]

    # ==================== QUOTE METHODS ====================
    def _generate_quote_number(self) -> str:
        date_str = datetime.now().strftime("%Y%m%d")
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM quotes WHERE quote_number LIKE ?", (f"QT-{date_str}-%",))
            count = cursor.fetchone()[0] + 1
        return f"QT-{date_str}-{count:04d}"

    def create_quote(self, user_id: str, client_id: str, items: List[Dict],
                     issue_date: str = None, valid_until: str = None,
                     notes: str = None, status: str = "draft") -> Dict:
        if not issue_date:
            issue_date = datetime.now().isoformat()
        if not valid_until:
            valid_until = (datetime.now() + timedelta(days=14)).isoformat()

        quote_id = str(uuid.uuid4())[:8]
        quote_number = self._generate_quote_number()

        subtotal = sum(item["quantity"] * item["unit_price"] for item in items)
        vat = subtotal * config.VAT_RATE
        total = subtotal + vat

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO quotes (id, user_id, client_id, quote_number, issue_date, valid_until,
                                   subtotal, vat, total, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (quote_id, user_id, client_id, quote_number, issue_date, valid_until,
                  subtotal, vat, total, status, notes))

            for item in items:
                amount = item["quantity"] * item["unit_price"]
                cursor.execute("""
                    INSERT INTO quote_items (quote_id, description, quantity, unit_price, amount)
                    VALUES (?, ?, ?, ?, ?)
                """, (quote_id, item["description"], item["quantity"], item["unit_price"], amount))

            conn.commit()

        return {
            "id": quote_id,
            "quote_number": quote_number,
            "subtotal": subtotal,
            "vat": vat,
            "total": total,
            "status": status,
            "message": f"Quote {quote_number} created"
        }

    def get_quotes(self, user_id: str, status: str = None, limit: int = 50) -> List[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            query = """
                SELECT q.id, q.quote_number, c.name as client_name, q.issue_date, q.valid_until,
                       q.subtotal, q.vat, q.total, q.status, q.created_at
                FROM quotes q
                JOIN clients c ON q.client_id = c.id
                WHERE q.user_id = ?
            """
            params = [user_id]
            if status:
                query += " AND q.status = ?"
                params.append(status)
            query += " ORDER BY q.created_at DESC LIMIT ?"
            params.append(limit)
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [{
            "id": r[0], "quote_number": r[1], "client_name": r[2],
            "issue_date": r[3], "valid_until": r[4], "subtotal": r[5],
            "vat": r[6], "total": r[7], "status": r[8], "created_at": r[9]
        } for r in rows]

    def convert_quote_to_invoice(self, user_id: str, quote_id: str) -> Dict:
        quote = self.get_quote(user_id, quote_id)
        if not quote:
            return {"error": "Quote not found"}

        # Get items
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT description, quantity, unit_price FROM quote_items WHERE quote_id = ?", (quote_id,))
            items = [{"description": r[0], "quantity": r[1], "unit_price": r[2]} for r in cursor.fetchall()]

        # Create invoice
        invoice = self.create_invoice(
            user_id=user_id,
            client_id=quote["client_id"],
            items=items,
            issue_date=datetime.now().isoformat(),
            due_date=(datetime.now() + timedelta(days=30)).isoformat(),
            notes=quote.get("notes"),
            status="draft"
        )

        # Update quote status
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE quotes SET status = 'converted' WHERE user_id = ? AND id = ?",
                          (user_id, quote_id))
            conn.commit()

        return {"invoice": invoice, "message": f"Quote {quote['quote_number']} converted to invoice {invoice['invoice_number']}"}

    def get_quote(self, user_id: str, quote_id: str) -> Optional[Dict]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT q.id, q.quote_number, q.client_id, c.name as client_name,
                       c.email, c.phone, c.address,
                       q.issue_date, q.valid_until, q.subtotal, q.vat, q.total,
                       q.status, q.notes, q.created_at
                FROM quotes q
                JOIN clients c ON q.client_id = c.id
                WHERE q.user_id = ? AND q.id = ?
            """, (user_id, quote_id))
            row = cursor.fetchone()
            if not row:
                return None

            cursor.execute("SELECT description, quantity, unit_price FROM quote_items WHERE quote_id = ?", (quote_id,))
            items = [{"description": r[0], "quantity": r[1], "unit_price": r[2]} for r in cursor.fetchall()]

        return {
            "id": row[0], "quote_number": row[1], "client_id": row[2],
            "client_name": row[3], "client_email": row[4], "client_phone": row[5],
            "client_address": row[6], "issue_date": row[7], "valid_until": row[8],
            "subtotal": row[9], "vat": row[10], "total": row[11],
            "status": row[12], "notes": row[13], "created_at": row[14], "items": items
        }

    # ==================== DELETE METHODS — FIXED: cascade to children ====================
    def delete_all_data(self, user_id: str, confirm: bool = False) -> Dict:
        if not confirm:
            return {"error": "Delete all requires confirm=True"}

        with self._get_conn() as conn:
            cursor = conn.cursor()

            # Get invoice IDs for this user
            cursor.execute("SELECT id FROM invoices WHERE user_id = ?", (user_id,))
            invoice_ids = [row[0] for row in cursor.fetchall()]

            # Delete invoice items and payments for these invoices
            for inv_id in invoice_ids:
                cursor.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (inv_id,))
                cursor.execute("DELETE FROM payments WHERE invoice_id = ?", (inv_id,))

            # Get quote IDs for this user
            cursor.execute("SELECT id FROM quotes WHERE user_id = ?", (user_id,))
            quote_ids = [row[0] for row in cursor.fetchall()]

            # Delete quote items
            for qt_id in quote_ids:
                cursor.execute("DELETE FROM quote_items WHERE quote_id = ?", (qt_id,))

            # Now delete parent records
            cursor.execute("DELETE FROM invoices WHERE user_id = ?", (user_id,))
            i_count = cursor.rowcount
            cursor.execute("DELETE FROM quotes WHERE user_id = ?", (user_id,))
            q_count = cursor.rowcount
            cursor.execute("DELETE FROM clients WHERE user_id = ?", (user_id,))
            c_count = cursor.rowcount

            conn.commit()

        return {"deleted": {"invoices": i_count, "quotes": q_count, "clients": c_count},
                "message": f"Deleted {i_count + q_count + c_count} records and all associated items"}

# ══════════════════════════════════════════════════════════════
# PDF GENERATION (Optional)
# ══════════════════════════════════════════════════════════════

async def generate_invoice_pdf(invoice_data: Dict, output_path: str) -> bool:
    """Generate PDF invoice if reportlab is available"""
    if not PDF_AVAILABLE:
        return False

    try:
        doc = SimpleDocTemplate(output_path, pagesize=A4, rightMargin=20*mm, leftMargin=20*mm,
                                topMargin=20*mm, bottomMargin=20*mm)
        styles = getSampleStyleSheet()
        story = []

        # Header
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=16, alignment=0)
        story.append(Paragraph(f"INVOICE", title_style))
        story.append(Spacer(1, 5*mm))

        # Company info
        company_style = ParagraphStyle('Company', parent=styles['Normal'], fontSize=10)
        story.append(Paragraph(f"<b>{config.COMPANY_NAME}</b>", company_style))
        story.append(Paragraph(f"Reg: {config.COMPANY_REG_NUMBER}", company_style))
        story.append(Paragraph(f"VAT: {config.COMPANY_VAT_NUMBER}", company_style))
        story.append(Spacer(1, 10*mm))

        # Invoice details
        invoice_info = [
            ["Invoice Number:", invoice_data.get("invoice_number", "")],
            ["Issue Date:", invoice_data.get("issue_date", "").split("T")[0]],
            ["Due Date:", invoice_data.get("due_date", "").split("T")[0]],
            ["Status:", invoice_data.get("status", "").upper()],
        ]
        info_table = Table(invoice_info, colWidths=[40*mm, 80*mm])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Courier'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(info_table)
        story.append(Spacer(1, 10*mm))

        # Client info
        story.append(Paragraph(f"<b>Bill To:</b>", styles['Normal']))
        story.append(Paragraph(invoice_data.get("client_name", ""), styles['Normal']))
        if invoice_data.get("client_email"):
            story.append(Paragraph(invoice_data.get("client_email", ""), styles['Normal']))
        if invoice_data.get("client_address"):
            story.append(Paragraph(invoice_data.get("client_address", ""), styles['Normal']))
        story.append(Spacer(1, 10*mm))

        # Items table
        items_data = [["Description", "Qty", "Unit Price", "Amount"]]
        for item in invoice_data.get("items", []):
            items_data.append([
                item.get("description", ""),
                f"{item.get('quantity', 0):.2f}",
                f"R{item.get('unit_price', 0):,.2f}",
                f"R{item.get('amount', 0):,.2f}"
            ])

        items_table = Table(items_data, colWidths=[70*mm, 20*mm, 30*mm, 30*mm])
        items_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Courier'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ]))
        story.append(items_table)
        story.append(Spacer(1, 10*mm))

        # Totals
        totals_data = [
            ["Subtotal:", f"R{invoice_data.get('subtotal', 0):,.2f}"],
            ["VAT (15%):", f"R{invoice_data.get('vat', 0):,.2f}"],
            ["TOTAL:", f"R{invoice_data.get('total', 0):,.2f}"],
        ]
        totals_table = Table(totals_data, colWidths=[120*mm, 30*mm])
        totals_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Courier'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ]))
        story.append(totals_table)

        # Notes
        if invoice_data.get("notes"):
            story.append(Spacer(1, 10*mm))
            story.append(Paragraph(f"<b>Notes:</b> {invoice_data.get('notes')}", styles['Normal']))

        doc.build(story)
        return True
    except Exception as e:
        log.error(f"PDF generation failed: {e}")
        return False

# ══════════════════════════════════════════════════════════════
# PYDANTIC MODELS (Input validation) — FIXED
# ══════════════════════════════════════════════════════════════

class AddClientRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    email: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=50)
    address: Optional[str] = Field(None, max_length=500)
    tax_number: Optional[str] = Field(None, max_length=50)

class UpdateClientRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    email: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=50)
    address: Optional[str] = Field(None, max_length=500)
    tax_number: Optional[str] = Field(None, max_length=50)

class InvoiceItem(BaseModel):
    description: str = Field(..., min_length=1, max_length=500)
    quantity: float = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)

class CreateInvoiceRequest(BaseModel):
    client_id: str = Field(..., min_length=1)
    items: List[InvoiceItem] = Field(..., min_length=1)
    issue_date: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    due_date: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    notes: Optional[str] = Field(None, max_length=1000)
    status: str = Field("draft", pattern="^(draft|sent|paid|overdue|cancelled)$")

class CreateQuoteRequest(BaseModel):
    client_id: str = Field(..., min_length=1)
    items: List[InvoiceItem] = Field(..., min_length=1)
    issue_date: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    valid_until: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    notes: Optional[str] = Field(None, max_length=1000)

class AddPaymentRequest(BaseModel):
    invoice_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    payment_date: Optional[str] = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    payment_method: Optional[str] = Field(None, max_length=50)
    reference: Optional[str] = Field(None, max_length=100)

# FIXED: added invoice_id
class UpdateInvoiceStatusRequest(BaseModel):
    invoice_id: str = Field(..., min_length=1)
    status: str = Field(..., pattern="^(draft|sent|paid|overdue|cancelled)$")

# FIXED: added client_id
class DeleteClientRequest(BaseModel):
    client_id: str = Field(..., min_length=1)
    confirm: bool = Field(False, description="Must be true to delete")

class DeleteAllRequest(BaseModel):
    confirm: bool = Field(False, description="Must be true to delete all data")

# ══════════════════════════════════════════════════════════════
# AUTH — STRICT
# ══════════════════════════════════════════════════════════════

async def get_current_user(authorization: str = Header(default="")) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")

    if authorization.startswith("Bearer "):
        token = authorization[7:]
        if not config.JWT_SECRET:
            raise HTTPException(status_code=401, detail="JWT not configured")
        try:
            payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token: missing subject")
            return user_id
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    else:
        if not config.API_KEY:
            raise HTTPException(status_code=401, detail="API key not configured")
        if not hmac.compare_digest(authorization, config.API_KEY):
            raise HTTPException(status_code=401, detail="Invalid API key")
        return "api_user"

# ══════════════════════════════════════════════════════════════
# MCP TOOL IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════════

async def tool_add_client(user_id: str, name: str, email: str = None, phone: str = None,
                          address: str = None, tax_number: str = None) -> Dict:
    return db.add_client(user_id, name, email, phone, address, tax_number)

async def tool_get_clients(user_id: str) -> Dict:
    return {"clients": db.get_clients(user_id)}

async def tool_get_client(user_id: str, client_id: str) -> Dict:
    client = db.get_client(user_id, client_id)
    if not client:
        return {"error": "Client not found"}
    return client

async def tool_update_client(user_id: str, client_id: str, **kwargs) -> Dict:
    updated = db.update_client(user_id, client_id, **kwargs)
    if not updated:
        return {"error": "Client not found or no changes"}
    return {"message": "Client updated", "client_id": client_id}

async def tool_delete_client(user_id: str, client_id: str, confirm: bool = False) -> Dict:
    return db.delete_client(user_id, client_id, confirm)

async def tool_create_invoice(user_id: str, client_id: str, items: List[Dict],
                              issue_date: str = None, due_date: str = None,
                              notes: str = None, status: str = "draft") -> Dict:
    return db.create_invoice(user_id, client_id, items, issue_date, due_date, notes, status)

async def tool_get_invoices(user_id: str, status: str = None, limit: int = 50) -> Dict:
    return {"invoices": db.get_invoices(user_id, status, limit)}

async def tool_get_invoice(user_id: str, invoice_id: str) -> Dict:
    invoice = db.get_invoice(user_id, invoice_id)
    if not invoice:
        return {"error": "Invoice not found"}
    return invoice

async def tool_update_invoice_status(user_id: str, invoice_id: str, status: str) -> Dict:
    updated = db.update_invoice_status(user_id, invoice_id, status)
    if not updated:
        return {"error": "Invoice not found"}
    return {"message": f"Invoice status updated to {status}", "invoice_id": invoice_id}

async def tool_add_payment(user_id: str, invoice_id: str, amount: float,
                           payment_date: str = None, payment_method: str = None,
                           reference: str = None) -> Dict:
    return db.add_payment(user_id, invoice_id, amount, payment_date, payment_method, reference)

async def tool_get_payments(user_id: str, invoice_id: str = None) -> Dict:
    return {"payments": db.get_payments(user_id, invoice_id)}

async def tool_create_quote(user_id: str, client_id: str, items: List[Dict],
                            issue_date: str = None, valid_until: str = None,
                            notes: str = None) -> Dict:
    return db.create_quote(user_id, client_id, items, issue_date, valid_until, notes)

async def tool_get_quotes(user_id: str, status: str = None, limit: int = 50) -> Dict:
    return {"quotes": db.get_quotes(user_id, status, limit)}

async def tool_get_quote(user_id: str, quote_id: str) -> Dict:
    quote = db.get_quote(user_id, quote_id)
    if not quote:
        return {"error": "Quote not found"}
    return quote

async def tool_convert_quote_to_invoice(user_id: str, quote_id: str) -> Dict:
    return db.convert_quote_to_invoice(user_id, quote_id)

async def tool_generate_invoice_pdf(user_id: str, invoice_id: str) -> Dict:
    if not PDF_AVAILABLE:
        return {"error": "PDF generation not available. Install reportlab: pip install reportlab"}

    invoice = db.get_invoice(user_id, invoice_id)
    if not invoice:
        return {"error": "Invoice not found"}

    output_path = f"/tmp/invoice_{invoice_id}.pdf"
    success = await generate_invoice_pdf(invoice, output_path)
    if not success:
        return {"error": "PDF generation failed"}

    return {"pdf_path": output_path, "message": f"Invoice PDF generated at {output_path}"}

async def tool_delete_all_data(user_id: str, confirm: bool = False) -> Dict:
    return db.delete_all_data(user_id, confirm)

async def tool_get_revenue_summary(user_id: str, year: int = None) -> Dict:
    """Get revenue summary: total invoiced, collected, outstanding, overdue"""
    if not year:
        year = datetime.now().year
    invoices = db.get_invoices(user_id, limit=1000)
    year_invoices = [i for i in invoices if str(year) in (i.get("issue_date") or "")]

    total_invoiced  = sum(i.get("total", 0) for i in year_invoices)
    total_paid      = sum(i.get("amount_paid", 0) for i in year_invoices)
    outstanding     = sum(i.get("balance_due", 0) for i in year_invoices if i.get("status") not in ("paid", "cancelled"))
    overdue_invoices = [i for i in year_invoices if i.get("status") == "overdue"]
    overdue_amount   = sum(i.get("balance_due", 0) for i in overdue_invoices)

    by_client = {}
    for inv in year_invoices:
        cid = inv.get("client_id", "unknown")
        if cid not in by_client:
            by_client[cid] = {"invoiced": 0, "paid": 0}
        by_client[cid]["invoiced"] += inv.get("total", 0)
        by_client[cid]["paid"]     += inv.get("amount_paid", 0)

    return {
        "year":            year,
        "total_invoiced":  round(total_invoiced, 2),
        "total_collected": round(total_paid, 2),
        "outstanding":     round(outstanding, 2),
        "overdue_amount":  round(overdue_amount, 2),
        "overdue_count":   len(overdue_invoices),
        "collection_rate": round((total_paid / total_invoiced * 100) if total_invoiced > 0 else 0, 1),
        "invoice_count":   len(year_invoices),
        "by_client":       by_client
    }

async def tool_get_overdue_invoices(user_id: str) -> Dict:
    """Get all overdue invoices with days overdue"""
    invoices = db.get_invoices(user_id, limit=1000)
    now = datetime.now()
    overdue = []
    for inv in invoices:
        if inv.get("status") in ("paid", "cancelled"):
            continue
        due_date_str = inv.get("due_date")
        if not due_date_str:
            continue
        try:
            due_date = datetime.fromisoformat(due_date_str.replace("Z", "+00:00"))
            if due_date.replace(tzinfo=None) < now:
                days_overdue = (now - due_date.replace(tzinfo=None)).days
                inv["days_overdue"] = days_overdue
                overdue.append(inv)
        except Exception:
            continue
    overdue.sort(key=lambda x: x.get("days_overdue", 0), reverse=True)
    total_overdue = sum(i.get("balance_due", 0) for i in overdue)
    return {
        "overdue_invoices": overdue,
        "count": len(overdue),
        "total_overdue_amount": round(total_overdue, 2)
    }

async def tool_get_client_statement(user_id: str, client_id: str) -> Dict:
    """Get full financial statement for a client: invoices, payments, balance"""
    client = db.get_client(user_id, client_id)
    if not client:
        return {"error": "Client not found"}

    invoices = db.get_invoices(user_id, limit=1000)
    client_invoices = [i for i in invoices if i.get("client_id") == client_id]
    payments = db.get_payments(user_id)
    client_payments = [p for p in payments if any(i.get("invoice_id") == p.get("invoice_id") for i in client_invoices)]

    total_invoiced = sum(i.get("total", 0) for i in client_invoices)
    total_paid     = sum(p.get("amount", 0) for p in client_payments)
    balance        = total_invoiced - total_paid

    return {
        "client":          client,
        "total_invoiced":  round(total_invoiced, 2),
        "total_paid":      round(total_paid, 2),
        "balance_owing":   round(balance, 2),
        "invoice_count":   len(client_invoices),
        "payment_count":   len(client_payments),
        "invoices":        client_invoices[:20],
        "payments":        client_payments[:20]
    }

# ══════════════════════════════════════════════════════════════
# TOOL REGISTRY — FIXED JSON SCHEMAS
# ══════════════════════════════════════════════════════════════

TOOLS = {
    "add_client": {
        "name": "add_client",
        "description": "Add a new client",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "address": {"type": "string"},
                "tax_number": {"type": "string"}
            },
            "required": ["name"]
        },
        "handler": tool_add_client
    },
    "get_clients": {
        "name": "get_clients",
        "description": "Get all clients",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_get_clients
    },
    "get_client": {
        "name": "get_client",
        "description": "Get a specific client by ID",
        "inputSchema": {"type": "object", "properties": {"client_id": {"type": "string"}}, "required": ["client_id"]},
        "handler": tool_get_client
    },
    "update_client": {
        "name": "update_client",
        "description": "Update client information",
        "inputSchema": {"type": "object", "properties": {"client_id": {"type": "string"}}, "required": ["client_id"]},
        "handler": tool_update_client
    },
    "delete_client": {
        "name": "delete_client",
        "description": "Delete a client. Requires confirm=true",
        "inputSchema": {"type": "object", "properties": {"client_id": {"type": "string"}, "confirm": {"type": "boolean", "default": False}}, "required": ["client_id", "confirm"]},
        "handler": tool_delete_client
    },
    "create_invoice": {
        "name": "create_invoice",
        "description": "Create a new invoice",
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string"},
                            "quantity": {"type": "number"},
                            "unit_price": {"type": "number"}
                        },
                        "required": ["description", "quantity", "unit_price"]
                    }
                },
                "issue_date": {"type": "string"},
                "due_date": {"type": "string"},
                "notes": {"type": "string"},
                "status": {"type": "string", "enum": ["draft", "sent", "paid", "overdue", "cancelled"], "default": "draft"}
            },
            "required": ["client_id", "items"]
        },
        "handler": tool_create_invoice
    },
    "get_invoices": {
        "name": "get_invoices",
        "description": "Get all invoices",
        "inputSchema": {"type": "object", "properties": {"status": {"type": "string"}, "limit": {"type": "integer", "default": 50}}},
        "handler": tool_get_invoices
    },
    "get_invoice": {
        "name": "get_invoice",
        "description": "Get a specific invoice by ID",
        "inputSchema": {"type": "object", "properties": {"invoice_id": {"type": "string"}}, "required": ["invoice_id"]},
        "handler": tool_get_invoice
    },
    "update_invoice_status": {
        "name": "update_invoice_status",
        "description": "Update invoice status (draft/sent/paid/overdue/cancelled)",
        "inputSchema": {"type": "object", "properties": {"invoice_id": {"type": "string"}, "status": {"type": "string", "enum": ["draft", "sent", "paid", "overdue", "cancelled"]}}, "required": ["invoice_id", "status"]},
        "handler": tool_update_invoice_status
    },
    "add_payment": {
        "name": "add_payment",
        "description": "Record a payment against an invoice",
        "inputSchema": {
            "type": "object",
            "properties": {
                "invoice_id": {"type": "string"},
                "amount": {"type": "number"},
                "payment_date": {"type": "string"},
                "payment_method": {"type": "string"},
                "reference": {"type": "string"}
            },
            "required": ["invoice_id", "amount"]
        },
        "handler": tool_add_payment
    },
    "get_payments": {
        "name": "get_payments",
        "description": "Get payments (optionally for a specific invoice)",
        "inputSchema": {"type": "object", "properties": {"invoice_id": {"type": "string"}}},
        "handler": tool_get_payments
    },
    "create_quote": {
        "name": "create_quote",
        "description": "Create a new quote",
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string"},
                            "quantity": {"type": "number"},
                            "unit_price": {"type": "number"}
                        },
                        "required": ["description", "quantity", "unit_price"]
                    }
                },
                "issue_date": {"type": "string"},
                "valid_until": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["client_id", "items"]
        },
        "handler": tool_create_quote
    },
    "get_quotes": {
        "name": "get_quotes",
        "description": "Get all quotes",
        "inputSchema": {"type": "object", "properties": {"status": {"type": "string"}, "limit": {"type": "integer", "default": 50}}},
        "handler": tool_get_quotes
    },
    "get_quote": {
        "name": "get_quote",
        "description": "Get a specific quote by ID",
        "inputSchema": {"type": "object", "properties": {"quote_id": {"type": "string"}}, "required": ["quote_id"]},
        "handler": tool_get_quote
    },
    "convert_quote_to_invoice": {
        "name": "convert_quote_to_invoice",
        "description": "Convert a quote to an invoice",
        "inputSchema": {"type": "object", "properties": {"quote_id": {"type": "string"}}, "required": ["quote_id"]},
        "handler": tool_convert_quote_to_invoice
    },
    "generate_invoice_pdf": {
        "name": "generate_invoice_pdf",
        "description": "Generate PDF for an invoice (requires reportlab)",
        "inputSchema": {"type": "object", "properties": {"invoice_id": {"type": "string"}}, "required": ["invoice_id"]},
        "handler": tool_generate_invoice_pdf
    },
    "get_revenue_summary": {
        "name": "get_revenue_summary",
        "description": "Get annual revenue summary: total invoiced, collected, outstanding, overdue amount, collection rate, and breakdown by client.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "description": "Year to summarize (defaults to current year)"}
            }
        },
        "handler": tool_get_revenue_summary
    },
    "get_overdue_invoices": {
        "name": "get_overdue_invoices",
        "description": "Get all overdue invoices sorted by days overdue. Includes total overdue amount.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_get_overdue_invoices
    },
    "get_client_statement": {
        "name": "get_client_statement",
        "description": "Get a full financial statement for a client including all invoices, payments, and current balance owing.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_id": {"type": "string", "description": "Client ID"}
            },
            "required": ["client_id"]
        },
        "handler": tool_get_client_statement
    },
        "delete_all_data": {
        "name": "delete_all_data",
        "description": "Delete ALL data for this user. Requires confirm=true",
        "inputSchema": {"type": "object", "properties": {"confirm": {"type": "boolean", "default": False}}, "required": ["confirm"]},
        "handler": tool_delete_all_data
    }
}

# ══════════════════════════════════════════════════════════════
# MCP MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════

async def handle_mcp_message(message: Dict, user_id: str) -> Dict:
    msg_id = message.get("id")
    method = message.get("method", "")
    params = message.get("params", {})

    def ok(result):
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def err(code, msg, data=None):
        error = {"code": code, "message": msg}
        if data:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": msg_id, "error": error}

    try:
        if method == "initialize":
            return ok({
                "protocolVersion": config.PROTOCOL_VERSION,
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "serverInfo": {"name": config.SERVER_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR}
            })
        elif method in ("initialized", "ping"):
            return ok({})
        elif method == "tools/list":
            return ok({"tools": [{"name": t["name"], "description": t["description"], "inputSchema": t["inputSchema"]} for t in TOOLS.values()]})
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            if tool_name not in TOOLS:
                return err(-32601, f"Tool not found: {tool_name}")

            # Validate input
            try:
                if tool_name == "add_client":
                    validated = AddClientRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "update_client":
                    validated = UpdateClientRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "create_invoice":
                    validated = CreateInvoiceRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "create_quote":
                    validated = CreateQuoteRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "add_payment":
                    validated = AddPaymentRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "update_invoice_status":
                    validated = UpdateInvoiceStatusRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "delete_client":
                    validated = DeleteClientRequest(**tool_args)
                    args = validated.model_dump()
                elif tool_name == "delete_all_data":
                    validated = DeleteAllRequest(**tool_args)
                    args = validated.model_dump()
                else:
                    args = tool_args
            except Exception as e:
                return err(-32602, f"Invalid arguments: {e}")

            # Rate limit
            tool_limit = config.RATE_LIMIT_WRITE if tool_name in ["add_client", "delete_client", "create_invoice", "add_payment", "delete_all_data"] else config.RATE_LIMIT_DEFAULT
            key = rate_limiter.get_key(user_id, tool_name)
            allowed, _ = await rate_limiter.check(key, tool_limit)
            if not allowed:
                return err(-32000, "Rate limit exceeded")

            result = await TOOLS[tool_name]["handler"](user_id, **args)
            return ok({
                "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
                "isError": "error" in result
            })
        else:
            return err(-32601, f"Method not found: {method}")
    except Exception as e:
        log.error(f"MCP handler error: {e}", exc_info=True)
        return err(-32603, "Internal error", {"detail": str(e)})

# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    await rate_limiter.connect()
    if not PDF_AVAILABLE:
        log.warning("ReportLab not installed — PDF generation disabled. Install: pip install reportlab")
    log.info(f"{config.DISPLAY_NAME} v{config.SERVER_VERSION} ready")
    yield
    log.info(f"{config.DISPLAY_NAME} shutting down")

app = FastAPI(title=config.DISPLAY_NAME, version=config.SERVER_VERSION, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    return {"status": "online", "service": config.DISPLAY_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR}

@app.get("/")
async def root():
    return {"name": config.DISPLAY_NAME, "version": config.SERVER_VERSION, "vendor": config.VENDOR, "tools": list(TOOLS.keys())}

@app.post("/mcp")
async def mcp_endpoint(request: Request, user_id: str = Depends(get_current_user)):
    client_id = request.client.host if request.client else "unknown"
    allowed, _ = await rate_limiter.check(rate_limiter.get_key(client_id, "mcp"), config.RATE_LIMIT_DEFAULT)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    response = await handle_mcp_message(body, user_id)
    return JSONResponse(content=response)

# ══════════════════════════════════════════════════════════════
# STDIO TRANSPORT
# ══════════════════════════════════════════════════════════════

async def run_stdio():
    log.info(f"{config.DISPLAY_NAME} — stdio mode")
    await rate_limiter.connect()
    if not PDF_AVAILABLE:
        log.warning("ReportLab not installed — PDF generation disabled")

    user_id = os.getenv("FOREMAN_DEFAULT_USER")
    if not user_id:
        log.error("FOREMAN_DEFAULT_USER not set for stdio mode")
        sys.exit(1)

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        try:
            line = await reader.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as e:
                error_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"Parse error: {e}"}}
                sys.stdout.write(json.dumps(error_resp) + "\n")
                sys.stdout.flush()
                continue
            response = await handle_mcp_message(message, user_id)
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
        except asyncio.CancelledError:
            break
        except Exception as e:
            error_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
            sys.stdout.write(json.dumps(error_resp) + "\n")
            sys.stdout.flush()

# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

def main():
    try:
        config.validate()
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    args = sys.argv[1:]

    if "--stdio" in args:
        asyncio.run(run_stdio())
    else:
        port = config.PORT
        for i, arg in enumerate(args):
            if arg == "--port" and i + 1 < len(args):
                port = int(args[i + 1])

        print(f"\n{'='*60}")
        print(f"  {config.DISPLAY_NAME} MCP v{config.SERVER_VERSION}")
        print(f"  {config.VENDOR}")
        print(f"{'='*60}")
        print(f"  HTTP:      http://0.0.0.0:{port}")
        print(f"  MCP:       http://0.0.0.0:{port}/mcp")
        print(f"  Health:    http://0.0.0.0:{port}/health")
        print(f"  Tools:     {', '.join(TOOLS.keys())}")
        print(f"  Features:  Client management")
        print(f"             Invoice generation (PDF optional)")
        print(f"             Quote management")
        print(f"             Payment tracking")
        print(f"             VAT calculation (15%)")
        print(f"             SQLite WAL mode")
        print(f"             Confirm required for delete")
        print(f"{'='*60}\n")

        uvicorn.run(app, host=config.HOST, port=port, log_level="info")

if __name__ == "__main__":
    main()