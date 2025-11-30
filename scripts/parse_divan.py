#!/usr/bin/env python3
"""
Парсер цен на диваны с сайта divan.ru

Скрипт для учебных целей — домашнее задание AZ03.
Собирает цены с каталога диванов и сохраняет в CSV.

Использование:
    python scripts/parse_divan.py

Результат:
    data/divan_prices_raw.csv
"""

import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ============================================================
# КОНФИГУРАЦИЯ — можно менять под свои нужды
# ============================================================

BASE_URL = "https://www.divan.ru/category/divany"
PAGES_TO_FETCH = 2          # Количество страниц каталога (1–5 для учебных целей)
REQUEST_DELAY = 1.5         # Пауза между запросами (секунды)
REQUEST_TIMEOUT = 15        # Таймаут запроса (секунды)

# Регулярное выражение для поиска цен вида "68 990 руб." или "от 45 990 руб."
PRICE_PATTERN = re.compile(r"\d[\d\s]+руб\.")

# Заголовки запроса — указываем, что это учебный скрипт
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 "
        "(AZ03-homework-bot/1.0; educational purposes)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

# Путь для сохранения данных
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "divan_prices_raw.csv"


# ============================================================
# ФУНКЦИИ
# ============================================================

def fetch_page(url: str) -> str | None:
    """
    Загружает HTML-страницу по URL.
    
    Возвращает текст страницы или None при ошибке.
    """
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"  ⚠️  Ошибка при загрузке {url}: {e}")
        return None


def extract_prices_from_html(html: str) -> list[str]:
    """
    Извлекает строки с ценами из HTML-страницы.
    
    Использует BeautifulSoup для получения текста страницы,
    затем ищет все вхождения паттерна цены.
    
    Возвращает список уникальных строк вида "68 990 руб."
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Получаем весь текст страницы
    page_text = soup.get_text(" ", strip=True)
    
    # Ищем все совпадения с паттерном цены
    matches = PRICE_PATTERN.findall(page_text)
    
    # Убираем дубликаты, сохраняя порядок
    seen = set()
    unique_prices: list[str] = []
    for price in matches:
        if price not in seen:
            seen.add(price)
            unique_prices.append(price)
    
    return unique_prices


def build_page_url(base_url: str, page: int) -> str:
    """Формирует URL для конкретной страницы каталога."""
    if page == 1:
        return base_url
    return f"{base_url}?page={page}"


def collect_prices(base_url: str, pages: int, delay: float) -> list[str]:
    """
    Собирает цены с нескольких страниц каталога.
    
    Args:
        base_url: Базовый URL каталога
        pages: Количество страниц для обхода
        delay: Пауза между запросами (секунды)
    
    Returns:
        Список строк с ценами
    """
    all_prices: list[str] = []
    
    print(f"\n{'='*50}")
    print(f"Парсинг цен с {base_url}")
    print(f"Страниц для обхода: {pages}")
    print(f"{'='*50}\n")
    
    for page_num in range(1, pages + 1):
        url = build_page_url(base_url, page_num)
        print(f"📄 Страница {page_num}/{pages}: {url}")
        
        html = fetch_page(url)
        if html is None:
            print(f"  ⏭️  Пропускаю страницу {page_num}")
            continue
        
        page_prices = extract_prices_from_html(html)
        print(f"  ✅ Найдено цен: {len(page_prices)}")
        
        all_prices.extend(page_prices)
        
        # Пауза перед следующим запросом (кроме последней страницы)
        if page_num < pages:
            print(f"  ⏳ Пауза {delay} сек...")
            time.sleep(delay)
    
    print(f"\n{'='*50}")
    print(f"Всего собрано ценовых строк: {len(all_prices)}")
    print(f"{'='*50}\n")
    
    return all_prices


def save_to_csv(prices: list[str], output_path: Path) -> None:
    """
    Сохраняет список цен в CSV-файл.
    
    Формат: одна колонка "price_raw" со строками цен.
    """
    # Создаём директорию, если её нет
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Записываем CSV вручную (без pandas для минимизации зависимостей)
    with open(output_path, "w", encoding="utf-8-sig") as f:
        f.write("price_raw\n")
        for price in prices:
            # Экранируем кавычки, если есть
            escaped = price.replace('"', '""')
            f.write(f'"{escaped}"\n')
    
    print(f"💾 Данные сохранены в: {output_path.resolve()}")


# ============================================================
# ТОЧКА ВХОДА
# ============================================================

def main() -> int:
    """Главная функция скрипта."""
    print("\n🛋️  Парсер цен на диваны (divan.ru)")
    print("    Учебный проект AZ03\n")
    
    # Собираем цены
    prices = collect_prices(
        base_url=BASE_URL,
        pages=PAGES_TO_FETCH,
        delay=REQUEST_DELAY
    )
    
    if not prices:
        print("❌ Не удалось собрать цены. Проверь подключение к интернету")
        print("   или актуальность селекторов (сайт мог изменить вёрстку).")
        return 1
    
    # Сохраняем в CSV
    save_to_csv(prices, OUTPUT_FILE)
    
    print("\n✅ Готово! Теперь открой notebooks/az03_homework.ipynb")
    print("   для очистки данных и построения графиков.\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
