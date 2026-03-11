from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urljoin

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

BASE_URL = "https://www.amazon.com.br"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


@dataclass
class SearchItem:
    query: str
    page: int
    position: int
    asin: str
    title: str
    product_url: str
    sponsored: bool
    price: str | None = None
    rating: str | None = None
    review_count: str | None = None
    sold_by: str | None = None
    shipped_by: str | None = None
    marketplace_classification: str = "desconhecido"
    raw_merchant_text: str | None = None


def normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def maybe_text(locator) -> str | None:
    try:
        if locator.count() > 0:
            text = normalize_space(locator.first.inner_text(timeout=2000))
            return text or None
    except Exception:
        return None
    return None


def maybe_attr(locator, attr_name: str) -> str | None:
    try:
        if locator.count() > 0:
            value = locator.first.get_attribute(attr_name, timeout=2000)
            return normalize_space(value) or None
    except Exception:
        return None
    return None


def random_delay(page: Page, min_ms: int, max_ms: int) -> None:
    page.wait_for_timeout(random.randint(min_ms, max_ms))


def dismiss_cookie_banner(page: Page) -> None:
    selectors = [
        "input#sp-cc-accept",
        "input[name='accept']",
        "button:has-text('Aceitar')",
        "button:has-text('Accept')",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0 and locator.first.is_visible(timeout=1000):
                locator.first.click(timeout=1500)
                page.wait_for_timeout(500)
                return
        except Exception:
            continue


def looks_like_bot_block(page: Page) -> bool:
    try:
        title = normalize_space(page.title()).lower()
    except Exception:
        title = ""

    body_text = ""
    try:
        body_text = normalize_space(page.locator("body").inner_text(timeout=3000)).lower()
    except Exception:
        pass

    signals = [
        "not a robot",
        "não sou um robô",
        "digite os caracteres",
        "insira os caracteres",
        "503 service unavailable",
        "sorry, we just need to make sure you're not a robot",
        "robot check",
    ]
    haystack = f"{title} {body_text}"
    return any(signal in haystack for signal in signals)


def is_sponsored(card) -> bool:
    selectors = [
        "span.puis-sponsored-label-text",
        "span.s-label-popover-default",
        "[aria-label*='Patrocinado']",
        "[aria-label*='Sponsored']",
        "span:has-text('Patrocinado')",
        "a:has-text('Patrocinado')",
        "span:has-text('Sponsored')",
        "a:has-text('Sponsored')",
    ]
    for selector in selectors:
        try:
            locator = card.locator(selector)
            if locator.count() > 0:
                return True
        except Exception:
            continue

    try:
        text = normalize_space(card.inner_text(timeout=2000)).lower()
        if "patrocinado" in text or "sponsored" in text:
            return True
    except Exception:
        pass

    return False


def extract_asin_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", url)
    if match:
        return match.group(1)
    return None


def classify_marketplace(sold_by: str | None, shipped_by: str | None) -> str:
    sold = normalize_space(sold_by).lower()
    shipped = normalize_space(shipped_by).lower()

    sold_is_amazon = "amazon" in sold
    shipped_is_amazon = "amazon" in shipped

    if sold_is_amazon:
        return "amazon"
    if sold and shipped_is_amazon:
        return "terceiro_fba"
    if sold and shipped and not shipped_is_amazon:
        return "terceiro_mfn"
    if sold and not shipped:
        return "terceiro"
    if not sold and shipped_is_amazon:
        return "amazon_ou_fba"
    return "desconhecido"


def extract_price(card) -> str | None:
    candidates = [
        "span.a-price > span.a-offscreen",
        ".a-price .a-offscreen",
        "span[data-a-color='price'] .a-offscreen",
    ]
    for selector in candidates:
        text = maybe_text(card.locator(selector))
        if text:
            return text
    return None


def extract_card_item(query: str, card, page_no: int, position: int) -> SearchItem | None:
    asin = normalize_space(card.get_attribute("data-asin") or "")
    if not asin:
        return None

    title = None
    for selector in [
        "h2 a span",
        "h2 span",
        "a.a-link-normal h2 span",
        "span.a-size-base-plus.a-color-base.a-text-normal",
    ]:
        title = maybe_text(card.locator(selector))
        if title:
            break

    link = maybe_attr(card.locator("h2 a"), "href")
    product_url = urljoin(BASE_URL, link) if link else ""
    asin = asin or extract_asin_from_url(product_url) or ""

    rating = None
    for selector in ["span.a-icon-alt", "i.a-icon-star-small span.a-icon-alt"]:
        rating = maybe_text(card.locator(selector))
        if rating:
            break

    review_count = None
    for selector in [
        "span[aria-label$='avaliações']",
        "a[href*='#customerReviews'] span.a-size-base",
        "span.a-size-base.s-underline-text",
    ]:
        review_count = maybe_text(card.locator(selector))
        if review_count:
            break

    if not title:
        return None

    return SearchItem(
        query=query,
        page=page_no,
        position=position,
        asin=asin,
        title=title,
        product_url=product_url,
        sponsored=is_sponsored(card),
        price=extract_price(card),
        rating=rating,
        review_count=review_count,
    )


def extract_merchant_text(page: Page) -> str:
    selectors = [
        "#merchantInfo",
        "#tabular-buybox",
        "#shipsFromSoldBy_feature_div",
        "#fulfilledByAmazon_feature_div",
        "#exports_desktop_merchant_info_feature_div",
        "#desktop_qualifiedBuyBox",
        "#buybox",
    ]

    pieces: list[str] = []
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                text = normalize_space(locator.first.inner_text(timeout=2000))
                if text:
                    pieces.append(text)
        except Exception:
            continue

    if pieces:
        return "\n".join(dict.fromkeys(pieces))

    try:
        return normalize_space(page.locator("body").inner_text(timeout=4000))
    except Exception:
        return ""


def parse_merchant_info(text: str) -> tuple[str | None, str | None, str | None]:
    clean = normalize_space(text)
    if not clean:
        return None, None, None

    sold_by: str | None = None
    shipped_by: str | None = None

    both_patterns = [
        r"Enviado de e vendido por\s+(.+?)(?:\.|$)",
        r"Vendido e entregue por\s+(.+?)(?:\.|$)",
    ]
    for pattern in both_patterns:
        match = re.search(pattern, clean, flags=re.IGNORECASE)
        if match:
            entity = normalize_space(match.group(1))
            sold_by = entity
            shipped_by = entity
            return sold_by, shipped_by, clean

    sold_patterns = [
        r"Vendido por\s+(.+?)(?=\s+(?:Enviado por|Entregue por|Pagamento|Política|Adicionar|Comprar|Novo|Usado|Cor|Tamanho|Marca|$))",
        r"Seller\s+(.+?)(?=\s+(?:Ships from|Sold by|$))",
    ]
    for pattern in sold_patterns:
        match = re.search(pattern, clean, flags=re.IGNORECASE)
        if match:
            sold_by = normalize_space(match.group(1))
            break

    ship_patterns = [
        r"Enviado por\s+(.+?)(?=\s+(?:Vendido por|Pagamento|Política|Adicionar|Comprar|Novo|Usado|Cor|Tamanho|Marca|$))",
        r"Entregue por\s+(.+?)(?=\s+(?:Vendido por|Pagamento|Política|Adicionar|Comprar|Novo|Usado|Cor|Tamanho|Marca|$))",
        r"Ships from\s+(.+?)(?=\s+(?:Sold by|$))",
    ]
    for pattern in ship_patterns:
        match = re.search(pattern, clean, flags=re.IGNORECASE)
        if match:
            shipped_by = normalize_space(match.group(1))
            break

    if not shipped_by and re.search(r"Enviado pela Amazon|Entregue pela Amazon", clean, flags=re.IGNORECASE):
        shipped_by = "Amazon.com.br"

    return sold_by, shipped_by, clean


def enrich_item_with_marketplace(page: Page, item: SearchItem, min_delay_ms: int, max_delay_ms: int) -> SearchItem:
    if not item.product_url:
        return item

    try:
        page.goto(item.product_url, wait_until="domcontentloaded", timeout=60000)
        dismiss_cookie_banner(page)
        random_delay(page, min_delay_ms, max_delay_ms)

        if looks_like_bot_block(page):
            item.raw_merchant_text = "BLOCKED"
            item.marketplace_classification = "bloqueado"
            return item

        merchant_text = extract_merchant_text(page)
        sold_by, shipped_by, raw = parse_merchant_info(merchant_text)
        item.sold_by = sold_by
        item.shipped_by = shipped_by
        item.raw_merchant_text = raw
        item.marketplace_classification = classify_marketplace(sold_by, shipped_by)
    except PlaywrightTimeoutError:
        item.marketplace_classification = "timeout"
    except Exception as exc:
        item.marketplace_classification = f"erro: {exc.__class__.__name__}"

    return item


def scrape_amazon_search(
    query: str,
    pages: int,
    headless: bool,
    resolve_marketplace: bool,
    min_delay_ms: int,
    max_delay_ms: int,
) -> dict:
    items: list[SearchItem] = []
    seen: set[tuple[int, str]] = set()
    detail_cache: dict[str, tuple[str | None, str | None, str, str]] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            locale="pt-BR",
            user_agent=DEFAULT_USER_AGENT,
            viewport={"width": 1440, "height": 2400},
            timezone_id="America/Sao_Paulo",
        )
        page = context.new_page()
        detail_page = context.new_page() if resolve_marketplace else None

        for page_no in range(1, pages + 1):
            search_url = f"{BASE_URL}/s?k={quote_plus(query)}&page={page_no}"
            print(f"[INFO] Abrindo busca: {search_url}", file=sys.stderr)
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            dismiss_cookie_banner(page)
            random_delay(page, min_delay_ms, max_delay_ms)

            if looks_like_bot_block(page):
                raise RuntimeError(
                    "A Amazon retornou captcha/503/bloqueio. Tente reduzir a taxa, usar proxy/residencial "
                    "ou executar com --headful para validação manual."
                )

            cards = page.locator("div[data-component-type='s-search-result']")
            total_cards = cards.count()
            print(f"[INFO] Página {page_no}: {total_cards} cards encontrados", file=sys.stderr)

            for idx in range(total_cards):
                card = cards.nth(idx)
                item = extract_card_item(query=query, card=card, page_no=page_no, position=idx + 1)
                if not item:
                    continue

                key = (page_no, item.asin or item.product_url)
                if key in seen:
                    continue
                seen.add(key)

                if resolve_marketplace and detail_page and item.asin:
                    cached = detail_cache.get(item.asin)
                    if cached:
                        item.sold_by, item.shipped_by, item.marketplace_classification, item.raw_merchant_text = cached
                    else:
                        item = enrich_item_with_marketplace(
                            page=detail_page,
                            item=item,
                            min_delay_ms=min_delay_ms,
                            max_delay_ms=max_delay_ms,
                        )
                        detail_cache[item.asin] = (
                            item.sold_by,
                            item.shipped_by,
                            item.marketplace_classification,
                            item.raw_merchant_text or "",
                        )

                items.append(item)

        browser.close()

    sponsored = [asdict(item) for item in items if item.sponsored]
    organic = [asdict(item) for item in items if not item.sponsored]

    return {
        "query": query,
        "base_url": BASE_URL,
        "pages_requested": pages,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_items": len(items),
            "sponsored": len(sponsored),
            "organic": len(organic),
        },
        "sponsored_items": sponsored,
        "organic_items": organic,
    }


def write_json(payload: dict, output_path: Path) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def flatten_items(groups: Iterable[dict]) -> list[dict]:
    rows: list[dict] = []
    for item in groups:
        rows.append(item)
    return rows


def write_csv(rows: list[dict], output_path: Path) -> None:
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scraping da busca da Amazon Brasil e separação entre itens patrocinados e orgânicos, "
            "incluindo classificação do marketplace/seller."
        )
    )
    parser.add_argument("--query", required=True, help="Termo de busca, ex.: 'fone bluetooth'")
    parser.add_argument("--pages", type=int, default=2, help="Quantidade de páginas da busca")
    parser.add_argument(
        "--output-prefix",
        default="amazon_resultado",
        help="Prefixo para os arquivos de saída (.json e .csv)",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Executa com o navegador visível (útil quando a Amazon pedir captcha).",
    )
    parser.add_argument(
        "--no-marketplace",
        action="store_true",
        help="Não abre a página de detalhe de cada produto para classificar seller/marketplace.",
    )
    parser.add_argument(
        "--min-delay-ms",
        type=int,
        default=1200,
        help="Espera mínima entre navegações, em milissegundos.",
    )
    parser.add_argument(
        "--max-delay-ms",
        type=int,
        default=2200,
        help="Espera máxima entre navegações, em milissegundos.",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.pages < 1:
        parser.error("--pages deve ser >= 1")
    if args.min_delay_ms < 0 or args.max_delay_ms < 0:
        parser.error("Delays devem ser >= 0")
    if args.min_delay_ms > args.max_delay_ms:
        parser.error("--min-delay-ms não pode ser maior que --max-delay-ms")

    payload = scrape_amazon_search(
        query=args.query,
        pages=args.pages,
        headless=not args.headful,
        resolve_marketplace=not args.no_marketplace,
        min_delay_ms=args.min_delay_ms,
        max_delay_ms=args.max_delay_ms,
    )

    prefix = Path(args.output_prefix)
    json_path = prefix.with_suffix(".json")
    sponsored_csv_path = prefix.with_name(f"{prefix.name}_patrocinados.csv")
    organic_csv_path = prefix.with_name(f"{prefix.name}_organicos.csv")

    write_json(payload, json_path)
    write_csv(flatten_items(payload["sponsored_items"]), sponsored_csv_path)
    write_csv(flatten_items(payload["organic_items"]), organic_csv_path)

    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(f"[OK] JSON: {json_path}")
    print(f"[OK] CSV patrocinados: {sponsored_csv_path}")
    print(f"[OK] CSV orgânicos: {organic_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
