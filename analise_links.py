# -*- coding: utf-8 -*-
"""
Sistema de Monitoramento de Frete e Prazo - Magazine Luiza
Extrai informações de frete, prazo e preços (Otimizado e Paralelizado)

Autor: Ezequiel Dannus (Refatorado para Performance)
"""

import os
import re
import time
import random
import threading
import queue
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from sendemail.sendemail import enviar_email

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================


@dataclass
class Config:
    """Configurações centralizadas"""

    input_xlsx: str = r"guarda roupa magalu 25.02.xlsx"
    sheet_name: str = "Planilha1"
    cep: str = "01449010"
    headless: bool = False
    timeout: int = 10  # Reduzido de 20 para 10s (eager load é rápido)
    email_to: str = "ezequiel@madesa.com"
    email_cc: str = "v.ost@madesa.com"

    # Performance
    max_workers: int = 3  # Número de abas/navegadores simultâneos
    use_firefox_profile: bool = False  # Setar True se quiser usar o profile

    # Anti-bloqueio (Delays bastante reduzidos; esperas espertas são mais rápidas)
    delay_between_requests: Tuple[float, float] = (0.5, 1.5)
    restart_browser_every: int = 50
    max_retries: int = 2

    @property
    def output_dir(self) -> str:
        return os.path.dirname(self.input_xlsx) or "."

    @property
    def report_filename(self) -> str:
        data = datetime.now().strftime("%d-%m-%Y")
        return f"resultados_{data}_{self.sheet_name}.xlsx"


# ============================================================================
# PARSER E RESULTADO
# ============================================================================


class PriceParser:
    @staticmethod
    def extract_installment_price(text: str) -> Optional[str]:
        if not text:
            return None
        text = text.strip()

        match = re.search(r"ou\s+([\d.,]+)\s+em\s+\d+x", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        match = re.search(r"R\$\s*([\d.,]+)\s+em\s+\d+x", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        match = re.search(r"(\d+)x\s+de\s+R?\$?\s*([\d.,]+)", text, re.IGNORECASE)
        if match:
            parcelas = int(match.group(1))
            valor_parcela = float(match.group(2).replace(".", "").replace(",", "."))
            total = parcelas * valor_parcela
            return f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        match = re.search(r"([\d.]+,\d{2})", text)
        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def parse_to_float(price_text: str) -> Optional[float]:
        if not price_text:
            return None
        try:
            cleaned = (
                str(price_text)
                .replace("R$", "")
                .replace("\xa0", "")
                .replace(" ", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            )
            return float(cleaned)
        except (ValueError, AttributeError):
            return None


@dataclass
class ScrapingResult:
    url: str
    preco_prazo: Optional[str] = None
    preco_vista: Optional[str] = None
    erro: Optional[str] = None
    status: str = None

    def to_dict(self) -> dict:
        return {
            "URL": self.url,
            "Status": self.status or "Erro",
            "P_prazo": self.preco_prazo or "-",
            "P_vista": self.preco_vista or "-",
            "Erro": self.erro or "",
        }


# ============================================================================
# WEBDRIVER MANAGER E CORE DE SCRAPING
# ============================================================================


class WebDriverManager:
    @staticmethod
    def setup_driver(
        headless: bool = False, use_profile: bool = False
    ) -> webdriver.Firefox:
        options = Options()
        if headless:
            options.add_argument("--headless=new")

        # Otimizações de Perfomance drásticas
        options.page_load_strategy = (
            "eager"  # Evita carregar imagens, CSS pesados e scripts secundários
        )
        options.set_preference(
            "permissions.default.image", 2
        )  # Bloqueia carregamento de imagens

        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
        ]
        options.set_preference("general.useragent.override", random.choice(user_agents))

        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)

        if use_profile:
            options.add_argument("-profile")
            options.add_argument(
                r"C:\Users\ezequiel\AppData\Roaming\Mozilla\Firefox\Profiles\1t1hi1f0.default-release"
            )

        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Firefox(options=options)

        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        driver.maximize_window()
        driver.set_page_load_timeout(30)

        return driver

    @staticmethod
    def wait_for_any(
        driver, locators: List[Tuple], timeout: int = 10, visible: bool = True
    ):
        condition = (
            EC.visibility_of_element_located
            if visible
            else EC.presence_of_element_located
        )
        for how, selector in locators:
            try:
                el = WebDriverWait(driver, timeout).until(condition((how, selector)))
                return el.text.strip()
            except TimeoutException:
                continue
        return None

    @staticmethod
    def wait_click(driver, locators: List[Tuple], timeout: int = 3):
        for how, selector in locators:
            try:
                el = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((how, selector))
                )
                return el
            except TimeoutException:
                continue
        return None


class ScraperWorker:
    """Worker indívidual do pool - Garante múltiplos Firefox sendo geridos ao mesmo tempo"""

    XPATHS = {
        "cookie_buttons": [
            (By.XPATH, "//*[contains(@class,'cookies')]/descendant::button[1]"),
            (By.XPATH, "//button[contains(., 'Aceitar') or contains(., 'Continuar')]"),
        ],
        "prazo_text": [
            (
                By.XPATH,
                "/html/body/div[1]/div/aside/div[2]/div/div/div[2]/div/div/div/p[1]",
            ),
            (
                By.XPATH,
                "//*[contains(text(), 'Receba') or contains(text(), 'Entrega')]",
            ),
        ],
        "preco_prazo": [
            (By.XPATH, "//*[@id='product']/aside/div[1]/div[1]/div/div/span[1]")
        ],
        "preco_vista": [
            (
                By.XPATH,
                "/html/body/div[1]/div/aside/div[1]/div[1]/div/div/div[2]/p/span[2]",
            )
        ],
    }

    def __init__(self, config: Config):
        self.config = config
        self.driver = None
        self.requests_count = 0

    def init_driver(self):
        if self.driver:
            self.driver.quit()
        self.driver = WebDriverManager.setup_driver(
            self.config.headless, self.config.use_firefox_profile
        )
        self.requests_count = 0

    def get_result(self, url: str) -> ScrapingResult:
        if not self.driver or self.requests_count >= self.config.restart_browser_every:
            self.init_driver()

        self.requests_count += 1

        for tentativa in range(self.config.max_retries):
            try:
                if tentativa > 0:
                    time.sleep(2.0)

                self.driver.get(url)

                self._handle_cookies()

                # Coleta rápida pelos XPATHs nativos (Não faz mais time.sleep, apenas espera inteligentemente)
                prazo_texto = WebDriverManager.wait_for_any(
                    self.driver, self.XPATHS["prazo_text"], self.config.timeout
                )
                preco_prazo_raw = WebDriverManager.wait_for_any(
                    self.driver, self.XPATHS["preco_prazo"], self.config.timeout
                )
                preco_vista = WebDriverManager.wait_for_any(
                    self.driver, self.XPATHS["preco_vista"], self.config.timeout
                )

                preco_prazo = PriceParser.extract_installment_price(preco_prazo_raw)
                status = "Funcionando" if prazo_texto or preco_prazo else "Problema"

                time.sleep(random.uniform(*self.config.delay_between_requests))

                return ScrapingResult(
                    url=url,
                    preco_prazo=preco_prazo,
                    preco_vista=preco_vista,
                    status=status,
                )

            except Exception as e:
                if tentativa == self.config.max_retries - 1:
                    return ScrapingResult(url=url, erro=str(e))
            finally:
                if self.driver:
                    self.driver.delete_all_cookies()

    def _handle_cookies(self):
        btn = WebDriverManager.wait_click(
            self.driver, self.XPATHS["cookie_buttons"], timeout=1
        )
        if btn:
            try:
                btn.click()
            except Exception:
                pass

    def close(self):
        if self.driver:
            self.driver.quit()





class MagaluPipeline:
    def __init__(self, config: Config):
        self.config = config
        self.results = []
        self.lock = threading.Lock()

    def worker_loop(self, url_queue: queue.Queue, finished_counter: dict):
        worker = ScraperWorker(self.config)
        while True:
            try:
                url = url_queue.get_nowait()
            except queue.Empty:
                break

            res = worker.get_result(url)

            with self.lock:
                self.results.append(res)
                finished_counter["count"] += 1
                progresso = finished_counter["count"]
                total = finished_counter["total"]

                if res.erro:
                    print(f"[{progresso}/{total}] ERRO: {res.erro} | URL: {res.url}")
                else:
                    print(
                        f"[{progresso}/{total}] OK | Status: {res.status} | URL: {res.url}"
                    )

            url_queue.task_done()

        worker.close()

    def run(self):
        print("=" * 70)
        print("Sistema de Monitoramento - Magazine Luiza (Multi-Threaded)")
        print("=" * 70)

        links = self._load_links()
        if not links:
            print("Nenhum link válido encontrado.")
            return

        print(f"\nIniciando scraping de {len(links)} links.")
        print(
            f"Instâncias simultâneas de navegador (Workers): {self.config.max_workers}"
        )

        if self.config.use_firefox_profile and self.config.max_workers > 1:
            print(
                "[AVISO] 'use_firefox_profile' está ativado. Vários processos acessando o mesmo profile causa travamento (Lock)."
            )
            print(
                "[AVISO] Reduzindo 'max_workers' para 1 para garantir a estabilidade do profile."
            )
            self.config.max_workers = 1

        url_queue = queue.Queue()
        for ln in links:
            url_queue.put(ln)

        finished_counter = {"count": 0, "total": len(links)}
        threads = []

        workers_count = min(self.config.max_workers, len(links))

        for _ in range(workers_count):
            t = threading.Thread(
                target=self.worker_loop, args=(url_queue, finished_counter)
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        df = pd.DataFrame([r.to_dict() for r in self.results])

        report_path = os.path.join(self.config.output_dir, self.config.report_filename)
        df.to_excel(report_path, index=False)
        print(f"\nRelatório salvo: {report_path}")

        self._send_email(report_path)
        self._print_summary(df)

    def _load_links(self) -> List[str]:
        try:
            df = pd.read_excel(
                self.config.input_xlsx, sheet_name=self.config.sheet_name
            )
            links = (
                df["Link"]
                .dropna()
                .astype(str)
                .str.strip()
                .replace({"": np.nan})
                .dropna()
                .tolist()
            )
            print(f"{len(links)} links carregados da aba '{self.config.sheet_name}'")
            return links
        except Exception as e:
            print(f"Erro ao carregar links: {e}")
            return []

    def _send_email(self, report_path: str):
        try:
            print("\nEnviando relatório por email...")
            enviar_email(
                report_path,
                self.config.email_to,
                self.config.email_cc,
                f" Web - {self.config.sheet_name}",
            )
            print("Email enviado com sucesso")
        except Exception as e:
            print(f"Erro ao enviar email: {e}")

    def _print_summary(self, df: pd.DataFrame):
        print("\n" + "=" * 70)
        print("RESUMO DO PROCESSAMENTO")
        print("=" * 70)
        print(f"Total de URLs: {len(df)}")
        print(f"Sucessos: {len(df[df['Erro'] == ''])}")
        print(f"Erros: {len(df[df['Erro'] != ''])}")
        print("=" * 70)


# ============================================================================
# MAIN
# ============================================================================


def main():
    config = Config(
        input_xlsx=r"guarda roupa magalu 25.02.xlsx",
        sheet_name="Planilha1",
        cep="01449010",
        headless=False,
        max_workers=3,
        use_firefox_profile=False,
    )

    pipeline = MagaluPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
