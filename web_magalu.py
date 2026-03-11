# -*- coding: utf-8 -*-
"""
Sistema de Monitoramento de Frete e Prazo - Magazine Luiza
Extrai informações de frete, prazo e preços

Autor: Ezequiel Dannus (Refatorado)
"""

import os
import re
import time
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from sendemail.sendemail import enviar_email

# CONFIGURAÇÕES


@dataclass
class Config:
    """Configurações centralizadas"""

    input_xlsx: str = r"C:\Users\ezequiel\webScraping- WEB\Conferências OST.xlsx"
    sheet_name: str = "MAGALU"
    cep: str = "01449010"
    headless: bool = False
    timeout: int = 20
    email_to: str = "ezequiel@madesa.com"
    email_cc: str = "v.ost@madesa.com"

    # Anti-bloqueio
    delay_between_requests: Tuple[float, float] = (2.5, 5.0)
    restart_browser_every: int = 15
    max_retries: int = 2

    @property
    def output_dir(self) -> str:
        return os.path.dirname(self.input_xlsx) or "."

    @property
    def report_filename(self) -> str:
        data = datetime.now().strftime("%d-%m-%Y")
        return f"resultados_{data}_{self.sheet_name}.xlsx"


# UTILITÁRIOS DE PARSING (Análise)


class PriceParser:

    @staticmethod
    def extract_installment_price(text: str) -> Optional[str]:
        """
        Extrai o valor do parcelamento do texto

        Exemplos:
            "ou 2.039,88 em 10x de 203,99 sem juros" -> "2.039,88"
            "R$ 1.234,56 em 5x" -> "1.234,56"
            "10x de R$ 100,00" -> "1.000,00"
        """
        if not text:
            return None

        text = text.strip()

        # Padrão 1: "ou VALOR em Nx"
        match = re.search(r"ou\s+([\d.,]+)\s+em\s+\d+x", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Padrão 2: "R$ VALOR em Nx"
        match = re.search(r"R\$\s*([\d.,]+)\s+em\s+\d+x", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Padrão 3: "Nx de R$ VALOR" (calcula total)
        match = re.search(r"(\d+)x\s+de\s+R?\$?\s*([\d.,]+)", text, re.IGNORECASE)
        if match:
            parcelas = int(match.group(1))
            valor_parcela = float(match.group(2).replace(".", "").replace(",", "."))
            total = parcelas * valor_parcela
            return f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        # Padrão 4: Apenas número com vírgula/ponto
        match = re.search(r"([\d.]+,\d{2})", text)
        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def parse_to_float(price_text: str) -> Optional[float]:
        """Converte texto de preço em float"""
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


class DateCalculator:
    """Calculador melhorado de dias úteis"""

    # Mapas de meses e dias da semana
    MESES = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }

    DIAS_SEMANA = {
        "segunda-feira": 0,
        "segunda": 0,
        "seg": 0,
        "terça-feira": 1,
        "terca-feira": 1,
        "terça": 1,
        "terca": 1,
        "ter": 1,
        "quarta-feira": 2,
        "quarta": 2,
        "qua": 2,
        "quinta-feira": 3,
        "quinta": 3,
        "qui": 3,
        "sexta-feira": 4,
        "sexta": 4,
        "sex": 4,
        "sábado": 5,
        "sabado": 5,
        "sab": 5,
        "domingo": 6,
        "dom": 6,
    }

    @classmethod
    def calcular_dias_uteis(cls, prazo_texto: Optional[str]) -> Optional[int]:
        """
        Calcula dias úteis a partir de texto descritivo

        Formatos suportados:
        - "amanhã"
        - "até 15 de março"
        - "até segunda-feira"
        - "Receba até segunda-feira, 24 de novembro"
        """
        if not prazo_texto:
            return None

        prazo_texto = prazo_texto.lower().strip()
        hoje = datetime.now().date()

        print(f"    Analisando prazo: '{prazo_texto}'")

        # Caso 1: "amanhã"
        if "amanhã" in prazo_texto or "amanha" in prazo_texto:
            return cls._calcular_dias_uteis_ate(hoje + timedelta(days=1))

        # Caso 2: "até DD de MÊS" ou "DD de MÊS"
        data_mes = cls._extrair_data_completa(prazo_texto)
        if data_mes:
            dias = cls._calcular_dias_uteis_ate(data_mes)
            print(
                f"    -> Data encontrada: {data_mes.strftime('%d/%m/%Y')} ({dias} dias úteis)"
            )
            return dias

        # Caso 3: Dia da semana
        data_semana = cls._extrair_dia_semana(prazo_texto)
        if data_semana:
            dias = cls._calcular_dias_uteis_ate(data_semana)
            print(
                f"    -> Dia da semana: {data_semana.strftime('%d/%m/%Y')} ({dias} dias úteis)"
            )
            return dias

        # Caso 4: Número de dias explícito (ex: "em 5 dias úteis")
        match = re.search(r"(\d+)\s*dias?\s*úteis?", prazo_texto)
        if match:
            dias = int(match.group(1))
            print(f"    -> Dias úteis explícitos: {dias}")
            return dias

        print("    -> Não foi possível calcular dias úteis")
        return None

    @classmethod
    def _calcular_dias_uteis_ate(cls, data_final: datetime.date) -> int:
        """Calcula dias úteis entre hoje e data_final"""
        hoje = datetime.now().date()

        # Se a data final é hoje ou antes, retorna 0
        if data_final <= hoje:
            return 0

        return int(np.busday_count(np.datetime64(hoje), np.datetime64(data_final)))

    @classmethod
    def _extrair_data_completa(cls, texto: str) -> Optional[datetime.date]:
        """
        Extrai data no formato 'DD de MÊS' ou 'até DD de MÊS'
        Ex: "até 24 de novembro", "Receba até segunda-feira, 24 de novembro"
        """
        # Padrão: qualquer texto com "DD de MÊS"
        pattern = r"(\d{1,2})\s+de\s+([a-zç]+)"
        match = re.search(pattern, texto, re.IGNORECASE)

        if not match:
            return None

        dia = int(match.group(1))
        mes_nome = match.group(2).lower()
        mes = cls.MESES.get(mes_nome)

        if not mes:
            return None

        hoje = datetime.now().date()
        ano = hoje.year

        try:
            data = datetime(ano, mes, dia).date()

            # Se a data já passou este ano, assumir ano seguinte
            if data < hoje:
                data = datetime(ano + 1, mes, dia).date()

            return data
        except ValueError:
            return None

    @classmethod
    def _extrair_dia_semana(cls, texto: str) -> Optional[datetime.date]:
        """
        Extrai próximo dia da semana mencionado
        Ex: "até segunda-feira", "entrega na quarta"
        """
        # Criar padrão regex com todos os nomes de dias
        pattern = r"\b(" + "|".join(cls.DIAS_SEMANA.keys()) + r")\b"
        match = re.search(pattern, texto)

        if not match:
            return None

        dia_nome = match.group(1)
        dia_semana_num = cls.DIAS_SEMANA.get(dia_nome)

        if dia_semana_num is None:
            return None

        hoje = datetime.now().date()
        hoje_semana = hoje.weekday()

        # Calcular dias até o próximo dia da semana
        dias_ate = (dia_semana_num - hoje_semana) % 7

        # Se é 0, significa que é hoje - vamos para a próxima semana
        if dias_ate == 0:
            dias_ate = 7

        return hoje + timedelta(days=dias_ate)


# DRIVER E SCRAPING


class WebDriverManager:
    """Gerencia WebDriver com anti-detecção"""

    @staticmethod
    def setup_driver(headless: bool = False) -> webdriver.Firefox:
        """Configura Firefox"""
        options = Options()

        if headless:
            options.add_argument("--headless=new")

        # User-agent rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
        ]
        options.set_preference("general.useragent.override", random.choice(user_agents))

        # Anti-detecção
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)

        # Performance
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Firefox(options=options)

        # Remover webdriver property
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        driver.maximize_window()
        driver.set_page_load_timeout(60)

        return driver

    @staticmethod
    def type_like_human(element, text: str):
        """Simula digitação humana"""
        element.clear()
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.2))

    @staticmethod
    def wait_for_any(
        driver,
        locators: List[Tuple],
        timeout: int = 10,
        get_text: bool = True,
        visible: bool = True,
    ):
        """Aguarda por qualquer elemento de uma lista"""
        condition = (
            EC.visibility_of_element_located
            if visible
            else EC.presence_of_element_located
        )

        for how, selector in locators:
            try:
                el = WebDriverWait(driver, timeout).until(condition((how, selector)))
                return el.text.strip() if get_text else el
            except TimeoutException:
                continue

        return None


@dataclass
class ScrapingResult:
    """Resultado do scraping de um produto"""

    url: str
    frete: Optional[str] = None
    prazo_dias: Optional[int] = None
    preco_prazo: Optional[str] = None
    preco_vista: Optional[str] = None
    erro: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "URL": self.url,
            "Frete": self.frete or "Erro",
            "Prazo": self.prazo_dias if self.prazo_dias is not None else "-",
            "P_prazo": self.preco_prazo or "-",
            "P_vista": self.preco_vista or "-",
            "Erro": self.erro or "",
        }


class MagaluScraper:
    """Scraper específico para Magazine Luiza"""

    XPATHS = {
        "cookie_buttons": [
            (By.XPATH, "//*[contains(@class,'cookies')]/descendant::button[1]"),
            (By.XPATH, "//button[contains(., 'Aceitar') or contains(., 'Continuar')]"),
        ],
        "toggle_cep": [
            (By.XPATH, "/html/body/div[1]/div/header/section/div[1]/div/button/span"),
        ],
        "cep_inputs": [
            (By.XPATH, "//input[@id='zipcode-input']"),
            (By.XPATH, "//input[@name='zipcode-input']"),
            (By.XPATH, "//input[@id='frete']"),
            (By.XPATH, "//input[contains(@placeholder,'CEP')]"),
        ],
        "submit_buttons": [
            (By.XPATH, "//button[contains(.,'OK') or contains(.,'Consultar')]")
        ],
        "frete_value": [
            (
                By.XPATH,
                "/html/body/div[1]/div/aside/div[2]/div/div/div[2]/div/div/div/p[2]",
            ),
            (By.XPATH, "//*[@id='freight-price-free']"),
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

    def __init__(self, driver, config: Config):
        self.driver = driver
        self.config = config
        self.wait = WebDriverManager.wait_for_any

    def scrape_all(self, links: List[str], cep: str) -> pd.DataFrame:
        """Faz scraping de todos os links"""
        resultados = []

        for i, url in enumerate(links, 1):
            print(f"\n[{i}/{len(links)}] Processando: {url}")

            # Reiniciar browser periodicamente
            if i > 1 and i % self.config.restart_browser_every == 0:
                print("  Reiniciando navegador...")
                self.driver.quit()
                time.sleep(random.uniform(3, 6))
                self.driver = WebDriverManager.setup_driver(self.config.headless)

            # Tentar com retries
            resultado = None
            for tentativa in range(self.config.max_retries):
                if tentativa > 0:
                    print(f"  Tentativa {tentativa + 1}/{self.config.max_retries}...")
                    time.sleep(random.uniform(5, 10))

                resultado = self._scrape_single(url, cep)

                if not resultado.erro:
                    break

            resultados.append(resultado.to_dict())

            # Log
            if resultado.erro:
                print(f"  ERRO: {resultado.erro}")
            else:
                print(
                    f"  OK | Frete: {resultado.frete} | Prazo: {resultado.prazo_dias} dias"
                )
                print(
                    f"     | Prazo: {resultado.preco_prazo} | Vista: {resultado.preco_vista}"
                )

            # Delay
            if i < len(links):
                delay = random.uniform(*self.config.delay_between_requests)
                print(f"  Aguardando {delay:.1f}s...")
                time.sleep(delay)

        return pd.DataFrame(resultados)

    def _scrape_single(self, url: str, cep: str) -> ScrapingResult:
        """Faz scraping de uma URL"""
        try:
            self.driver.get(url)
            time.sleep(random.uniform(2.5, 5.0))

            # Fechar cookies
            self._handle_cookies()

            # Preencher CEP
            if not self._fill_cep(cep):
                return ScrapingResult(url=url, erro="Campo CEP não encontrado")

            # Aguardar resultados
            time.sleep(random.uniform(2, 4))

            # Extrair dados
            frete = self.wait(
                self.driver, self.XPATHS["frete_value"], timeout=self.config.timeout
            )

            prazo_texto = self.wait(
                self.driver, self.XPATHS["prazo_text"], timeout=self.config.timeout
            )

            preco_prazo_raw = self.wait(
                self.driver, self.XPATHS["preco_prazo"], timeout=self.config.timeout
            )

            preco_vista = self.wait(
                self.driver, self.XPATHS["preco_vista"], timeout=self.config.timeout
            )

            # Processar preço a prazo
            preco_prazo = PriceParser.extract_installment_price(preco_prazo_raw)
            if preco_prazo:
                print(f"    Preço a prazo extraído: {preco_prazo}")
            else:
                print(f"    Preço a prazo raw: {preco_prazo_raw}")

            # Calcular dias úteis
            dias_uteis = DateCalculator.calcular_dias_uteis(prazo_texto)

            return ScrapingResult(
                url=url,
                frete=frete,
                prazo_dias=dias_uteis,
                preco_prazo=preco_prazo,
                preco_vista=preco_vista,
            )

        except Exception as e:
            return ScrapingResult(url=url, erro=str(e))
        finally:
            self.driver.delete_all_cookies()

    def _handle_cookies(self):
        """Fecha modal de cookies"""
        try:
            btn = self.wait(
                self.driver, self.XPATHS["cookie_buttons"], timeout=3, get_text=False
            )
            if btn:
                btn.click()
                time.sleep(random.uniform(1, 2))
        except Exception:
            pass

    def _fill_cep(self, cep: str) -> bool:
        """Preenche campo CEP"""
        # Tentar campo direto
        cep_input = self.wait(
            self.driver, self.XPATHS["cep_inputs"], timeout=10, get_text=False
        )

        # Se não achou, clicar no toggle
        if not cep_input:
            toggle = self.wait(
                self.driver, self.XPATHS["toggle_cep"], timeout=5, get_text=False
            )
            if toggle:
                toggle.click()
                time.sleep(random.uniform(1, 2))
                cep_input = self.wait(
                    self.driver, self.XPATHS["cep_inputs"], timeout=10, get_text=False
                )

        if not cep_input:
            return False

        # Preencher
        WebDriverManager.type_like_human(cep_input, cep)
        time.sleep(random.uniform(1.0, 2.5))

        # Submit
        try:
            cep_input.send_keys(Keys.RETURN)
        except Exception:
            btn = self.wait(
                self.driver, self.XPATHS["submit_buttons"], timeout=3, get_text=False
            )
            if btn:
                btn.click()

        return True


# ============================================================================
# PIPELINE
# ============================================================================


class MagaluPipeline:
    """Pipeline completo"""

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        """Executa pipeline"""
        print("=" * 70)
        print("Sistema de Monitoramento - Magazine Luiza")
        print("=" * 70)

        # Carregar links
        links = self._load_links()
        if not links:
            print("Nenhum link válido encontrado.")
            return

        # Setup driver
        print(f"\nConfigurando navegador (headless={self.config.headless})...")
        driver = WebDriverManager.setup_driver(self.config.headless)

        try:
            # Scraping
            print(f"\nIniciando scraping para CEP: {self.config.cep}\n")
            scraper = MagaluScraper(driver, self.config)
            df = scraper.scrape_all(links, self.config.cep)

            # Salvar
            report_path = os.path.join(
                self.config.output_dir, self.config.report_filename
            )
            df.to_excel(report_path, index=False)
            print(f"\nRelatório salvo: {report_path}")

            # Email
            self._send_email(report_path)

            # Resumo
            self._print_summary(df)

        finally:
            driver.quit()
            print("\nNavegador fechado")

    def _load_links(self) -> List[str]:
        """Carrega links da planilha"""
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
        """Envia email"""
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
        """Imprime resumo"""
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
        input_xlsx=r"C:\Users\ezequiel\webScraping- WEB\Conferências OST.xlsx",
        sheet_name="MAGALU",
        cep="01449010",
        headless=False,
    )

    pipeline = MagaluPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
