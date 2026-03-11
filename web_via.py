# -*- coding: utf-8 -*-
"""
Sistema de Monitoramento de Frete e Prazo de Entrega
Automatiza scraping de informações de frete e atualiza planilhas Excel

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
from selenium.common.exceptions import TimeoutException, WebDriverException

from sendemail.sendemail import enviar_email

# ============================================================================
# CONFIGURAÇÕES E CONSTANTES
# ============================================================================


@dataclass
class Config:
    """Configurações centralizadas do sistema"""

    input_xlsx: str = r"C:\Users\ezequiel\webScraping- WEB\Conferências OST.xlsx"
    sheet_name: str = "VIA"
    cep: str = "01449010"
    headless: bool = True
    timeout: int = 20
    page_load_timeout: int = 60
    email_to: str = "ezequiel@madesa.com"
    email_cc: str = "v.ost@madesa.com"

    # Configurações anti-bloqueio
    delay_between_requests: Tuple[float, float] = (3.0, 8.0)  # segundos (min, max)
    max_retries: int = 3
    retry_delay: Tuple[float, float] = (5.0, 10.0)
    restart_browser_every: int = 70  # Reiniciar navegador a cada N requisições
    use_proxy_rotation = (False,)
    proxies = [
        "189.126.66.189:8080",
        "191.252.204.220:8080",
        # adicione quantos quiser
    ]

    @property
    def output_dir(self) -> str:
        return os.path.dirname(self.input_xlsx) or "."

    @property
    def report_filename(self) -> str:
        data = datetime.now().strftime("%d-%m-%Y")
        return f"resultados_{data}_{self.sheet_name}.xlsx"


# Mapeamentos para cálculo de datas
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
    "terça-feira": 1,
    "terca-feira": 1,
    "quarta-feira": 2,
    "quinta-feira": 3,
    "sexta-feira": 4,
    "sábado": 5,
    "sabado": 5,
    "domingo": 6,
}


# ============================================================================
# UTILITÁRIOS DE DATA
# ============================================================================


class DateCalculator:
    """Calcula dias úteis baseado em textos de prazo"""

    @staticmethod
    def calcular_dias_uteis(prazo_texto: Optional[str]) -> Optional[int]:
        """
        Converte texto de prazo em número de dias úteis

        Args:
            prazo_texto: Texto descritivo do prazo (ex: "até 15 de março")

        Returns:
            Número de dias úteis ou None se não conseguir calcular
        """
        if not prazo_texto:
            return None

        prazo_texto = prazo_texto.lower().strip()
        hoje = datetime.now().date()

        # Caso 1: "amanhã"
        if "amanhã" in prazo_texto:
            return DateCalculator._calcular_dias_uteis_ate(hoje + timedelta(days=1))

        # Caso 2: "até DD de MÊS"
        data_entrega = DateCalculator._extrair_data_mes(prazo_texto)
        if data_entrega:
            return DateCalculator._calcular_dias_uteis_ate(data_entrega)

        # Caso 3: Dia da semana
        data_entrega = DateCalculator._extrair_dia_semana(prazo_texto)
        if data_entrega:
            return DateCalculator._calcular_dias_uteis_ate(data_entrega)

        return None

    @staticmethod
    def _calcular_dias_uteis_ate(data_final: datetime.date) -> int:
        """Calcula dias úteis entre amanhã e data_final"""
        hoje = datetime.now().date()
        return int(
            np.busday_count(
                np.datetime64(hoje + timedelta(days=1)),
                np.datetime64(data_final + timedelta(days=1)),
            )
        )

    @staticmethod
    def _extrair_data_mes(texto: str) -> Optional[datetime.date]:
        """Extrai data no formato 'até DD de MÊS'"""
        m = re.search(r"até\s+(\d{1,2})\s+de\s+([a-zç]+)", texto, re.IGNORECASE)
        if not m:
            return None

        dia = int(m.group(1))
        mes = MESES.get(m.group(2))
        if not mes:
            return None

        hoje = datetime.now().date()
        ano = hoje.year

        try:
            data = datetime(ano, mes, dia).date()
            if data < hoje:
                data = datetime(ano + 1, mes, dia).date()
            return data
        except ValueError:
            return None

    @staticmethod
    def _extrair_dia_semana(texto: str) -> Optional[datetime.date]:
        """Extrai próximo dia da semana mencionado"""
        pattern = r"\b(" + "|".join(DIAS_SEMANA.keys()) + r")\b"
        dias_encontrados = re.findall(pattern, texto)

        if not dias_encontrados:
            return None

        dia_semana_num = DIAS_SEMANA.get(dias_encontrados[0])
        if dia_semana_num is None:
            return None

        hoje = datetime.now().date()
        dias_ate = (dia_semana_num - hoje.weekday() + 7) % 7
        dias_ate = 7 if dias_ate == 0 else dias_ate

        return hoje + timedelta(days=dias_ate)


# ============================================================================
# DRIVER E SCRAPING
# ============================================================================


class WebDriverManager:
    """Gerencia o ciclo de vida do WebDriver"""

    @staticmethod
    def setup_driver(
        headless: bool = True, proxy: Optional[str] = None
    ) -> webdriver.Firefox:
        """Configura e retorna instância do Firefox WebDriver"""
        options = Options()

        if headless:
            options.add_argument("--headless=new")

        # Anti-detecção avançada
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        ]
        user_agent = random.choice(user_agents)
        options.set_preference("general.useragent.override", user_agent)

        # Ocultar automação
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)
        options.set_preference("marionette", False)

        # Configurações de privacidade
        options.set_preference("privacy.trackingprotection.enabled", True)
        options.set_preference("geo.enabled", False)
        options.set_preference("media.navigator.enabled", False)

        # Proxy (se fornecido)
        if proxy:
            ip, port = proxy.split(":")
            options.set_preference("network.proxy.type", 1)
            options.set_preference("network.proxy.http", ip)
            options.set_preference("network.proxy.http_port", int(port))
            options.set_preference("network.proxy.ssl", ip)
            options.set_preference("network.proxy.ssl_port", int(port))
            # Opcional: evitar DNS leak
            options.set_preference("network.proxy.socks_remote_dns", True)

        # Performance
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Firefox(options=options)

        # Remover propriedades do webdriver via JavaScript
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        driver.maximize_window()
        driver.set_page_load_timeout(60)

        return driver

    @staticmethod
    def simulate_human_behavior(driver):
        """Simula comportamento humano aleatório"""
        actions = [
            lambda: driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight/2);"
            ),
            lambda: driver.execute_script("window.scrollTo(0, 0);"),
            lambda: time.sleep(random.uniform(0.5, 1.5)),
        ]
        random.choice(actions)()

    @staticmethod
    def type_like_human(element, text: str):
        """Simula digitação humana com variação de velocidade"""
        element.clear()
        for i, char in enumerate(text):
            element.send_keys(char)
            # Variação: às vezes digita mais rápido, às vezes mais devagar
            if i % 3 == 0:
                time.sleep(random.uniform(0.1, 0.3))
            else:
                time.sleep(random.uniform(0.05, 0.15))

    @staticmethod
    def random_mouse_movement(driver):
        """Simula movimentos aleatórios do mouse"""
        try:
            from selenium.webdriver.common.action_chains import ActionChains

            actions = ActionChains(driver)
            for _ in range(random.randint(1, 3)):
                x_offset = random.randint(-100, 100)
                y_offset = random.randint(-100, 100)
                actions.move_by_offset(x_offset, y_offset)
                actions.perform()
                time.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass

    @staticmethod
    def wait_for_any_element(
        driver,
        locators: List[Tuple[str, str]],
        timeout: int = 10,
        get_text: bool = True,
        visible: bool = True,
    ):
        """Aguarda por qualquer elemento de uma lista de locators"""
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


# ============================================================================
# SCRAPER PRINCIPAL
# ============================================================================


@dataclass
class FreteResult:
    """Resultado de uma consulta de frete"""

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
            "Prazo": self.prazo_dias or "-",
            "P_prazo": self.preco_prazo or "-",
            "P_vista": self.preco_vista or "-",
            "Erro": self.erro or "",
        }


class FreteScraper:
    """Scraper de informações de frete"""

    # XPaths organizados por categoria
    XPATHS = {
        "cookie_buttons": [
            (By.XPATH, "//*[contains(@class,'cookies')]/descendant::button[1]"),
            (By.XPATH, "//button[contains(., 'Aceitar') or contains(., 'Continuar')]"),
        ],
        "cep_inputs": [
            (By.XPATH, "//input[@id='frete']"),
            (By.XPATH, "//input[@name='cep']"),
            (By.XPATH, "//input[contains(@placeholder,'CEP')]"),
            (By.XPATH, "//input[@type='tel' and contains(@aria-label,'CEP')]"),
        ],
        "toggle_buttons": [
            (
                By.XPATH,
                "//*[contains(.,'Consultar') and contains(.,'frete')]/ancestor::button",
            ),
            (By.XPATH, "//*[contains(.,'Alterar CEP')]/ancestor::button"),
        ],
        "submit_buttons": [
            (By.XPATH, "//*[@id='cep-box']/div[1]/div/form/div/button"),
            (By.XPATH, "//button[contains(.,'OK') or contains(.,'Consultar')]"),
        ],
        "frete_value": [
            (By.XPATH, "//*[@class='cep-item--value css-d17uor eym5xli0']"),
            (By.XPATH, "//*[@id='freight-price-free']"),
        ],
        "prazo_text": [
            (By.XPATH, "//*[@class='cep-item--days css-o6edse eym5xli0']"),
            (
                By.XPATH,
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'entrega')]",
            ),
        ],
        "preco_prazo": [
            (
                By.XPATH,
                "//*[@id='__next']/div/div[1]/div[2]/div/div[3]/div[4]/div/div/p/span[1]/span[2]",
            )
        ],
        "preco_vista": [(By.XPATH, "//*[@id='product-price']/span[2]")],
    }

    def __init__(self, driver, config: Config):
        self.driver = driver
        self.config = config
        self.wait = WebDriverManager.wait_for_any_element

    def scrape_all(self, links: List[str], cep: str) -> pd.DataFrame:
        """Faz scraping de todos os links com proteções anti-bloqueio"""
        resultados = []
        current_proxy_index = 0

        for i, url in enumerate(links, 1):
            print(f"\n[{i}/{len(links)}] Processando: {url}")

            # Reiniciar navegador periodicamente
            if i > 1 and i % self.config.restart_browser_every == 0:
                print("Reiniciando navegador para evitar detecção...")
                self.driver.quit()
                time.sleep(random.uniform(3, 6))

                # Rotacionar proxy se habilitado
                proxy = None
                if self.config.use_proxy_rotation and self.config.proxies:
                    proxy = self.config.proxies[
                        current_proxy_index % len(self.config.proxies)
                    ]
                    current_proxy_index += 1
                    print(f"Usando proxy: {proxy}")

                self.driver = WebDriverManager.setup_driver(
                    self.config.headless, proxy=proxy
                )

            # Tentar com retries
            resultado = None
            for tentativa in range(self.config.max_retries):
                if tentativa > 0:
                    delay = random.uniform(*self.config.retry_delay)
                    print(
                        f"Tentativa {tentativa + 1}/{self.config.max_retries} após {delay:.1f}s..."
                    )
                    time.sleep(delay)

                resultado = self._scrape_single(url, cep)

                # Se sucesso, sair do loop de retry
                if not resultado.erro or "bloqueio" not in resultado.erro.lower():
                    break

                # Se detectou bloqueio, esperar mais tempo
                if "bloqueio" in resultado.erro.lower():
                    print(f"Possível bloqueio detectado, aguardando...")
                    time.sleep(random.uniform(10, 20))

            resultados.append(resultado.to_dict())

            # Log do resultado
            if resultado.erro:
                print(f"ERRO: {resultado.erro}")
            else:
                print(f"Frete: {resultado.frete} | Prazo: {resultado.prazo_dias} dias")
                print(
                    f"P_prazo: {resultado.preco_prazo} | P_vista: {resultado.preco_vista}"
                )

            # Delay entre requisições (exceto na última)
            if i < len(links):
                delay = random.uniform(*self.config.delay_between_requests)
                print(f"Aguardando {delay:.1f}s antes da próxima requisição...")
                time.sleep(delay)

        return pd.DataFrame(resultados)

    def _scrape_single(self, url: str, cep: str) -> FreteResult:
        """Faz scraping de uma URL com comportamento humano"""
        try:
            self.driver.get(url)

            # Delay variável após carregar página
            time.sleep(random.uniform(3.0, 6.0))

            # Simular comportamento humano
            WebDriverManager.simulate_human_behavior(self.driver)

            # Fechar modal de cookies
            self._handle_cookie_modal()

            # Mais comportamento humano antes de interagir
            time.sleep(random.uniform(1.0, 2.5))
            WebDriverManager.random_mouse_movement(self.driver)

            # Encontrar e preencher campo CEP
            if not self._fill_cep_field(cep):
                return FreteResult(url=url, erro="Campo CEP não encontrado")

            # Aguardar carregamento dos resultados
            time.sleep(random.uniform(2.0, 4.0))

            # Extrair informações
            frete = self.wait(
                self.driver, self.XPATHS["frete_value"], timeout=self.config.timeout
            )
            prazo_texto = self.wait(
                self.driver, self.XPATHS["prazo_text"], timeout=self.config.timeout
            )
            preco_prazo = self.wait(
                self.driver, self.XPATHS["preco_prazo"], timeout=self.config.timeout
            )
            preco_vista = self.wait(
                self.driver, self.XPATHS["preco_vista"], timeout=self.config.timeout
            )

            dias_uteis = DateCalculator.calcular_dias_uteis(prazo_texto)

            return FreteResult(
                url=url,
                frete=frete,
                prazo_dias=dias_uteis,
                preco_prazo=preco_prazo,
                preco_vista=preco_vista,
            )

        except Exception as e:
            erro_msg = str(e)
            # Detectar possíveis bloqueios
            if any(
                keyword in erro_msg.lower()
                for keyword in ["timeout", "captcha", "403", "429"]
            ):
                erro_msg = f"Possível bloqueio: {erro_msg}"
            return FreteResult(url=url, erro=erro_msg)
        finally:
            self.driver.delete_all_cookies()

    def _handle_cookie_modal(self):
        """Fecha modal de cookies se aparecer"""
        try:
            btn = self.wait(
                self.driver, self.XPATHS["cookie_buttons"], timeout=3, get_text=False
            )
            if btn:
                btn.click()
                time.sleep(random.uniform(1, 2))
        except Exception:
            pass

    def _fill_cep_field(self, cep: str) -> bool:
        """Encontra, preenche e submete campo CEP com comportamento humano"""
        # Tentar encontrar campo direto
        cep_input = self.wait(
            self.driver, self.XPATHS["cep_inputs"], timeout=10, get_text=False
        )

        # Se não achou, tentar clicar no toggle primeiro
        if not cep_input:
            toggle = self.wait(
                self.driver, self.XPATHS["toggle_buttons"], timeout=5, get_text=False
            )
            if toggle:
                # Scroll até o elemento antes de clicar
                self.driver.execute_script("arguments[0].scrollIntoView(true);", toggle)
                time.sleep(random.uniform(0.5, 1.0))
                toggle.click()
                time.sleep(random.uniform(1.0, 2.0))

                cep_input = self.wait(
                    self.driver, self.XPATHS["cep_inputs"], timeout=10, get_text=False
                )

        if not cep_input:
            return False

        # Scroll até o campo CEP
        self.driver.execute_script("arguments[0].scrollIntoView(true);", cep_input)
        time.sleep(random.uniform(0.5, 1.0))

        # Simular clique humano no campo
        try:
            from selenium.webdriver.common.action_chains import ActionChains

            actions = ActionChains(self.driver)
            actions.move_to_element(cep_input).click().perform()
            time.sleep(random.uniform(0.3, 0.7))
        except Exception:
            cep_input.click()

        # Preencher CEP com digitação humana
        WebDriverManager.type_like_human(cep_input, cep)
        time.sleep(random.uniform(1.5, 3.0))

        # Submeter com pequena chance de usar Enter ou botão
        if random.random() < 0.7:  # 70% usa Enter
            cep_input.send_keys(Keys.RETURN)
        else:  # 30% clica no botão
            btn = self.wait(
                self.driver, self.XPATHS["submit_buttons"], timeout=3, get_text=False
            )
            if btn:
                time.sleep(random.uniform(0.5, 1.0))
                btn.click()
            else:
                cep_input.send_keys(Keys.RETURN)

        return True


# ============================================================================
# PROCESSAMENTO DE EXCEL
# ============================================================================


class ExcelProcessor:
    """Processa arquivos Excel"""

    @staticmethod
    def load_links(file_path: str, sheet_name: str) -> List[str]:
        """Carrega links da planilha"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)

            if "Link" not in df.columns:
                print(f"Coluna 'Link' não encontrada. Colunas: {df.columns.tolist()}")
                return []

            links = (
                df["Link"]
                .dropna()
                .astype(str)
                .str.strip()
                .replace({"": np.nan})
                .dropna()
                .tolist()
            )

            print(f"{len(links)} links carregados da aba '{sheet_name}'")
            return links

        except Exception as e:
            print(f"Erro ao carregar links: {e}")
            return []

    @staticmethod
    def save_report(df: pd.DataFrame, filepath: str):
        """Salva relatório em Excel"""
        try:
            df.to_excel(filepath, index=False)
            print(f"Relatório salvo: {filepath}")
        except Exception as e:
            print(f"Erro ao salvar relatório: {e}")
            raise


# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================


class FreteMonitorPipeline:
    """Pipeline completo de monitoramento"""

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        """Executa pipeline completo"""
        print("=" * 60)
        print("Iniciando Sistema de Monitoramento de Frete")
        print("=" * 60)

        # 1. Carregar links
        links = ExcelProcessor.load_links(
            self.config.input_xlsx, self.config.sheet_name
        )

        if not links:
            print("Nenhum link válido encontrado. Encerrando.")
            return

        # 2. Setup driver
        print(f"\nConfigurando navegador (headless={self.config.headless})...")
        driver = WebDriverManager.setup_driver(self.config.headless)

        try:
            # 3. Scraping
            print(f"\nIniciando scraping para CEP: {self.config.cep}")
            scraper = FreteScraper(driver, self.config)
            df_results = scraper.scrape_all(links, self.config.cep)

            # 4. Salvar relatório
            report_path = os.path.join(
                self.config.output_dir, self.config.report_filename
            )
            ExcelProcessor.save_report(df_results, report_path)

            # 5. Enviar email
            self._send_email_report(report_path)

            # 6. Resumo
            self._print_summary(df_results)

        finally:
            driver.quit()
            print("\nNavegador fechado")

    def _send_email_report(self, report_path: str):
        """Envia relatório por email"""
        try:
            print("\nEnviando relatório por email...")
            enviar_email(
                report_path,
                self.config.email_to,
                self.config.email_cc,
                f"Relatório de Frete e Prazo - {self.config.sheet_name}",
            )
            print("Email enviado com sucesso")
        except Exception as e:
            print(f"Erro ao enviar email: {e}")

    def _print_summary(self, df: pd.DataFrame):
        """Imprime resumo da execução"""
        print("\n" + "=" * 60)
        print("RESUMO DA EXECUÇÃO")
        print("=" * 60)
        print(f"Total de URLs processadas: {len(df)}")
        print(f"Sucessos: {len(df[df['Erro'] == ''])}")
        print(f"Erros: {len(df[df['Erro'] != ''])}")
        print("=" * 60)


# ============================================================================
# MAIN
# ============================================================================


def main():
    """Ponto de entrada principal"""
    config = Config(
        input_xlsx=r"C:\Users\ezequiel\webScraping- WEB\Conferências OST.xlsx",
        sheet_name="VIA",
        cep="01449010",
        headless=False,
    )

    pipeline = FreteMonitorPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
