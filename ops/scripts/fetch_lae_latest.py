#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga el último sorteo de PRIMITIVA, BONOLOTO, GORDO, EUROMILLONES
desde la web de LAE usando un proxy-caché público (r.jina.ai) para evitar 403,
y publica un JSON con el último sorteo por juego.
Salida: dist/lae_latest.json  (luego lo copiaremos a docs/api/)
"""

import os, re, json, datetime as dt
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

BASE = "https://r.jina.ai/http://www.loteriasyapuestas.es/"
PAGES = {
    "PRIMITIVA": "es/la-primitiva",
    "BONOLOTO":  "es/bonoloto",
    "GORDO":     "es/el-gordo-de-la-primitiva",
    "EURO":      "es/euromillones",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FranOps/1.0)"}

DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
NUM_RE  = re.compile(r"\b\d{1,2}\b")

def fetch_html(path):
    url = urljoin(BASE, path)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_draw(html, game):
    soup = BeautifulSoup(html, "html.parser")

    # Fecha (LAE muestra la última arriba; el proxy nos da el HTML renderizado)
    text
