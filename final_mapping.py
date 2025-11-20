#!/usr/bin/env python3
"""
Final JSDA Data Processor - Corrected mappings within actual column ranges
Addresses all column out-of-range issues to capture the final missing columns
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import re
import json
from typing import Dict, List, Optional, Any, Tuple
import numpy as np

# Suppress pandas warnings
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

def setup_logging():
    log_file = f"jsda_final_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger, log_file

logger, log_file = setup_logging()

class FinalJSDAProcessor:
    """Final JSDA processor with corrected column ranges and comprehensive mappings."""
    
    def __init__(self, config_path: str = None):
        logger.info("Initializing Final JSDA Processor...")

        # Load config file
        if config_path is None:
            config_path = Path(__file__).parent / "jsda_mapping_config.json"
        self._load_config(config_path)

        self._setup_target_structure()
        self._setup_sheet_mappings()
        self._setup_header_mapping_from_config()
        self._setup_final_mappings()
        logger.info("Processor initialized successfully")

    def _load_config(self, config_path):
        """Load mapping configuration from JSON file."""
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info(f"Loaded config from {config_path}")
        else:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            self.config = {}

    def _setup_header_mapping_from_config(self):
        """Build header mappings from config file."""
        if not self.config:
            self._setup_header_mapping()
            return

        self.header_to_target = {}
        header_mappings = self.config.get('header_mappings', {})

        for code, mapping in header_mappings.items():
            # Get all header patterns (Japanese and English)
            patterns = mapping.get('japanese', []) + mapping.get('english', [])
            num_col = mapping.get('number_col')
            amt_col = mapping.get('amount_col')

            for pattern in patterns:
                if num_col:
                    num_target = f'JPN.{code}.{{scope}}.{num_col}'
                else:
                    num_target = None
                if amt_col:
                    amt_target = f'JPN.{code}.{{scope}}.{amt_col}'
                else:
                    amt_target = None
                self.header_to_target[pattern] = (num_target, amt_target)

        # Load scope mapping from config
        self.sheet_scope = self.config.get('scope_mapping', {
            'domestic': 'DOM',
            'overseas': 'OVR',
            'total': 'TTL',
            'size_breakdown': 'SZ',
            'market_breakdown': 'MRKT',
            'reference': 'REF',
            'secondary_domestic': 'SDDOM',
            'secondary_overseas': 'SDOVR',
            'secondary_total': 'SDTTL',
            'secondary_size': 'SDSZ',
        })

        # Load column indicators
        self.column_indicators = self.config.get('column_indicators', {
            'number': ['件数', 'Number'],
            'amount': ['調達額', '売出額', 'Amount']
        })

        logger.info(f"Loaded {len(self.header_to_target)} header patterns from config")

    def _setup_header_mapping(self):
        """Map header text patterns to target column names for dynamic detection."""
        # This dictionary maps actual header text (Japanese/English) to target columns
        # Format: {header_text_pattern: (target_num_col, target_amt_col)}

        self.header_to_target = {
            # Domestic/Overseas/Total equity financing headers - Stocks Non-IPO
            '株券（新規上場以外）': ('JPN.SNIPO.{scope}.N.M', 'JPN.SNIPO.{scope}.A.M'),
            '株券等（新規公開以外）': ('JPN.SNIPO.{scope}.N.M', 'JPN.SNIPO.{scope}.A.M'),
            'Stocks(Non IPO)': ('JPN.SNIPO.{scope}.N.M', 'JPN.SNIPO.{scope}.A.M'),
            'Stocks,etc.(Shares,DR)': ('JPN.SNIPO.{scope}.N.M', 'JPN.SNIPO.{scope}.A.M'),
            'Non IPO': ('JPN.SNIPO.{scope}.N.M', 'JPN.SNIPO.{scope}.A.M'),

            # Convertible Bonds
            '転換社債型新株予約権付社債券': ('JPN.CB.{scope}.N.M', 'JPN.CB.{scope}.A.M'),
            '転換社債型': ('JPN.CB.{scope}.N.M', 'JPN.CB.{scope}.A.M'),
            'Convertible Bonds': ('JPN.CB.{scope}.N.M', 'JPN.CB.{scope}.A.M'),
            'CB': ('JPN.CB.{scope}.N.M', 'JPN.CB.{scope}.A.M'),

            # Bonds with Share Options
            '新株予約権付社債券': ('JPN.BWSO.{scope}.N.M', 'JPN.BWSO.{scope}.A.M'),
            '新株予約権付社債': ('JPN.BWSO.{scope}.N.M', 'JPN.BWSO.{scope}.A.M'),
            'Bonds with Share Options': ('JPN.BWSO.{scope}.N.M', 'JPN.BWSO.{scope}.A.M'),
            'BwSO': ('JPN.BWSO.{scope}.N.M', 'JPN.BWSO.{scope}.A.M'),

            # Share Option Certificates
            '新株予約権証券': ('JPN.SOC.{scope}.N.M', 'JPN.SOC.{scope}.A.M'),
            'Share Option Certificates': ('JPN.SOC.{scope}.N.M', 'JPN.SOC.{scope}.A.M'),
            'Share Option': ('JPN.SOC.{scope}.N.M', 'JPN.SOC.{scope}.A.M'),

            # Stocks IPO
            '株券等（新規上場）': ('JPN.SIPO.{scope}.N.M', 'JPN.SIPO.{scope}.A.M'),
            '株券等（新規公開）': ('JPN.SIPO.{scope}.N.M', 'JPN.SIPO.{scope}.A.M'),
            'Stocks(IPO)': ('JPN.SIPO.{scope}.N.M', 'JPN.SIPO.{scope}.A.M'),
            'IPO': ('JPN.SIPO.{scope}.N.M', 'JPN.SIPO.{scope}.A.M'),

            # Total Financing
            '調達額計': ('JPN.TF.{scope}.N.M', 'JPN.TF.{scope}.A.M'),
            '計': ('JPN.TF.{scope}.N.M', 'JPN.TF.{scope}.A.M'),
            'Total Domestic Financing': ('JPN.TF.{scope}.N.M', 'JPN.TF.{scope}.A.M'),
            'Total Overseas Financing': ('JPN.TF.{scope}.N.M', 'JPN.TF.{scope}.A.M'),
            'Total Financing': ('JPN.TF.{scope}.N.M', 'JPN.TF.{scope}.A.M'),

            # Market breakdown headers
            'スイスフラン建': ('JPN.SF.MRKT.N.M', 'JPN.SF.MRKT.A.M'),
            'スイスフラン': ('JPN.SF.MRKT.N.M', 'JPN.SF.MRKT.A.M'),
            'Swiss Franc': ('JPN.SF.MRKT.N.M', 'JPN.SF.MRKT.A.M'),

            'ユーロ建': ('JPN.EURL.MRKT.N.M', 'JPN.EURL.MRKT.A.M'),
            'ユーロ': ('JPN.EURL.MRKT.N.M', 'JPN.EURL.MRKT.A.M'),
            'Eurodollar': ('JPN.EURL.MRKT.N.M', 'JPN.EURL.MRKT.A.M'),
            'Euro': ('JPN.EURL.MRKT.N.M', 'JPN.EURL.MRKT.A.M'),

            'その他': ('JPN.OTH.MRKT.N.M', 'JPN.OTH.MRKT.A.M'),
            'Other Markets': ('JPN.OTH.MRKT.N.M', 'JPN.OTH.MRKT.A.M'),
            'Other': ('JPN.OTH.MRKT.N.M', 'JPN.OTH.MRKT.A.M'),

            # Reference headers
            '株主割当': ('JPN.SH.REF.N.M', 'JPN.SH.REF.A.M'),
            'Shareholder Allotment': ('JPN.SH.REF.N.M', 'JPN.SH.REF.A.M'),
            'Shareholder': ('JPN.SH.REF.N.M', 'JPN.SH.REF.A.M'),

            '第三者割当': ('JPN.TP.REF.N.M', 'JPN.TP.REF.A.M'),
            'Third Party': ('JPN.TP.REF.N.M', 'JPN.TP.REF.A.M'),

            # Secondary distribution headers - Non-IPO
            '株券等（新規公開以外）売出': ('JPN.NIPO.{scope}.N.M', 'JPN.NIPO.{scope}.A.M'),
            '新規公開以外': ('JPN.NIPO.{scope}.N.M', 'JPN.NIPO.{scope}.A.M'),

            # Secondary distribution headers - IPO
            '株券等（新規公開）売出': ('JPN.IPO.{scope}.N.M', 'JPN.IPO.{scope}.A.M'),
            '新規公開': ('JPN.IPO.{scope}.N.M', 'JPN.IPO.{scope}.A.M'),

            # Secondary distribution total
            '売出計': ('JPN.TTL.{scope}.N.M', 'JPN.TTL.{scope}.A.M'),

            # Size breakdown headers
            '10億円未満': ('JPN.L10B.{scope}.N.M', None),
            'Less than 10': ('JPN.L10B.{scope}.N.M', None),
            '10B未満': ('JPN.L10B.{scope}.N.M', None),

            '10億円以上50億円未満': ('JPN.L50B.{scope}.N.M', None),
            '50億円未満': ('JPN.L50B.{scope}.N.M', None),
            '10 billion yen to less than 50': ('JPN.L50B.{scope}.N.M', None),
            '10B以上50B未満': ('JPN.L50B.{scope}.N.M', None),

            '50億円以上100億円未満': ('JPN.L100B.{scope}.N.M', None),
            '100億円未満': ('JPN.L100B.{scope}.N.M', None),
            '50 billion yen to less than 100': ('JPN.L100B.{scope}.N.M', None),
            '50B以上100B未満': ('JPN.L100B.{scope}.N.M', None),

            '100億円以上': ('JPN.M100B.{scope}.N.M', None),
            '100 billion yen or more': ('JPN.M100B.{scope}.N.M', None),
            '100B以上': ('JPN.M100B.{scope}.N.M', None),

            '合計': ('JPN.TTL.{scope}.N.M', None),
            'Total': ('JPN.TTL.{scope}.N.M', None),
        }

        # Scope mapping for different sheet types
        self.sheet_scope = {
            'domestic': 'DOM',
            'overseas': 'OVR',
            'total': 'TTL',
            'size_breakdown': 'SZ',
            'market_breakdown': 'MRKT',
            'reference': 'REF',
            'secondary_domestic': 'SDDOM',
            'secondary_overseas': 'SDOVR',
            'secondary_total': 'SDTTL',
            'secondary_size': 'SDSZ',
        }

    def _setup_target_structure(self):
        """Define the exact target structure."""
        # Target columns in exact JSDA_DATA order
        self.target_columns = [
            "JPN.SNIPO.DOM.N.M", "JPN.SNIPO.DOM.A.M", "JPN.CB.DOM.N.M", "JPN.CB.DOM.A.M", 
            "JPN.BWSO.DOM.N.M", "JPN.BWSO.DOM.A.M", "JPN.SOC.DOM.N.M", "JPN.SOC.DOM.A.M", 
            "JPN.SIPO.DOM.N.M", "JPN.SIPO.DOM.A.M", "JPN.TF.DOM.N.M", "JPN.TF.DOM.A.M",
            "JPN.SNIPO.OVR.N.M", "JPN.SNIPO.OVR.A.M", "JPN.CB.OVR.N.M", "JPN.CB.OVR.A.M", 
            "JPN.BWSO.OVR.N.M", "JPN.BWSO.OVR.A.M", "JPN.SIPO.OVR.N.M", "JPN.SIPO.OVR.A.M", 
            "JPN.TF.OVR.N.M", "JPN.TF.OVR.A.M",
            "JPN.SNIPO.TTL.N.M", "JPN.SNIPO.TTL.A.M", "JPN.CB.TTL.N.M", "JPN.CB.TTL.A.M", 
            "JPN.BWSO.TTL.N.M", "JPN.BWSO.TTL.A.M", "JPN.SOC.TTL.N.M", "JPN.SOC.TTL.A.M", 
            "JPN.SIPO.TTL.N.M", "JPN.SIPO.TTL.A.M", "JPN.TF.TTL.N.M", "JPN.TF.TTL.A.M",
            "JPN.L10B.SZ.N.M", "JPN.L50B.SZ.N.M", "JPN.L100B.SZ.N.M", "JPN.M100B.SZ.N.M", "JPN.TTL.SZ.N.M",
            "JPN.SF.MRKT.N.M", "JPN.SF.MRKT.A.M", "JPN.EURL.MRKT.N.M", "JPN.EURL.MRKT.A.M", 
            "JPN.OTH.MRKT.N.M", "JPN.OTH.MRKT.A.M",
            "JPN.SH.REF.N.M", "JPN.SH.REF.A.M", "JPN.TP.REF.N.M", "JPN.TP.REF.A.M", 
            "JPN.SOC.REF.N.M", "JPN.SOC.REF.A.M",
            "JPN.NIPO.SDDOM.N.M", "JPN.NIPO.SDDOM.A.M", "JPN.IPO.SDDOM.N.M", "JPN.IPO.SDDOM.A.M", 
            "JPN.TTL.SDDOM.N.M", "JPN.TTL.SDDOM.A.M",
            "JPN.NIPO.SDOVR.N.M", "JPN.NIPO.SDOVR.A.M", "JPN.IPO.SDOVR.N.M", "JPN.IPO.SDOVR.A.M", 
            "JPN.TTL.SDOVR.N.M", "JPN.TTL.SDOVR.A.M",
            "JPN.NIPO.SDTTL.N.M", "JPN.NIPO.SDTTL.A.M", "JPN.IPO.SDTTL.N.M", "JPN.IPO.SDTTL.A.M", 
            "JPN.TTL.SDTTL.N.M", "JPN.TTL.SDTTL.A.M",
            "JPN.L10B.SDSZ.N.M", "JPN.L50B.SDSZ.N.M", "JPN.L100B.SDSZ.N.M", "JPN.M100B.SDSZ.N.M", "JPN.TTL.SDSZ.N.M"
        ]
        
        # Detailed descriptions matching JSDA_DATA format
        self.column_descriptions = {
            # Domestic
            'JPN.SNIPO.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Stocks(Non IPO) - Number',
            'JPN.SNIPO.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Stocks(Non IPO) - Amount',
            'JPN.CB.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Convertible Bonds - Number',
            'JPN.CB.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Convertible Bonds - Amount',
            'JPN.BWSO.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Number',
            'JPN.BWSO.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Amount',
            'JPN.SOC.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Share Option Certificates - Number',
            'JPN.SOC.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Share Option Certificates - Amount',
            'JPN.SIPO.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Stocks(IPO) - Number',
            'JPN.SIPO.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Stocks(IPO) - Amount',
            'JPN.TF.DOM.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Number',
            'JPN.TF.DOM.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Domestic/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Amount',
            
            # Overseas
            'JPN.SNIPO.OVR.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Stocks(Non IPO) - Number',
            'JPN.SNIPO.OVR.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Stocks(Non IPO) - Amount',
            'JPN.CB.OVR.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Convertible Bonds - Number',
            'JPN.CB.OVR.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Convertible Bonds - Amount',
            'JPN.BWSO.OVR.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Number',
            'JPN.BWSO.OVR.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Amount',
            'JPN.SIPO.OVR.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Stocks(IPO) - Number',
            'JPN.SIPO.OVR.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Stocks(IPO) - Amount',
            'JPN.TF.OVR.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Number',
            'JPN.TF.OVR.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Amount',
            
            # Total
            'JPN.SNIPO.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Stocks(Non IPO) - Number',
            'JPN.SNIPO.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Stocks(Non IPO) - Amount',
            'JPN.CB.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Convertible Bonds - Number',
            'JPN.CB.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Convertible Bonds - Amount',
            'JPN.BWSO.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Number',
            'JPN.BWSO.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Bonds with Share Options (excluding convertible bonds) - Amount',
            'JPN.SOC.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Share Option Certificates - Number',
            'JPN.SOC.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Share Option Certificates - Amount',
            'JPN.SIPO.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Stocks(IPO) - Number',
            'JPN.SIPO.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Stocks(IPO) - Amount',
            'JPN.TF.TTL.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Number',
            'JPN.TF.TTL.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Type); Trust Fund Beneficiary Certificates - Amount',
            
            # Size breakdown
            'JPN.L10B.SZ.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Size); Less than 10 billion yen - Number',
            'JPN.L50B.SZ.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Size); 10 billion yen to less than 50 billion yen - Number',
            'JPN.L100B.SZ.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Size); 50 billion yen to less than 100 billion yen - Number',
            'JPN.M100B.SZ.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Size); 100 billion yen or more - Number',
            'JPN.TTL.SZ.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Total/Breakdown by Issue Size); Total - Number',
            
            # Market breakdown
            'JPN.SF.MRKT.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Swiss Franc Markets - Number',
            'JPN.SF.MRKT.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Swiss Franc Markets - Amount',
            'JPN.EURL.MRKT.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Eurodollar Markets - Number',
            'JPN.EURL.MRKT.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Eurodollar Markets - Amount',
            'JPN.OTH.MRKT.N.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Other Markets - Number',
            'JPN.OTH.MRKT.A.M': 'Equity Financing by Companies Listed in Japan (Public Stock Offerings, etc./Overseas/Breakdown by Market); Other Markets - Amount',
            
            # Reference
            'JPN.SH.REF.N.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Shareholder Allotment - Number',
            'JPN.SH.REF.A.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Shareholder Allotment - Amount',
            'JPN.TP.REF.N.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Third Party Allotment - Number',
            'JPN.TP.REF.A.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Third Party Allotment - Amount',
            'JPN.SOC.REF.N.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Share Option Certificates - Number',
            'JPN.SOC.REF.A.M': 'Equity Financing by Companies Listed in Japan (Reference/Breakdown by Allotment Type); Share Option Certificates - Amount',
            
            # Secondary domestic
            'JPN.NIPO.SDDOM.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Stocks(Non IPO) - Number',
            'JPN.NIPO.SDDOM.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Stocks(Non IPO) - Amount',
            'JPN.IPO.SDDOM.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Stocks(IPO) - Number',
            'JPN.IPO.SDDOM.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Stocks(IPO) - Amount',
            'JPN.TTL.SDDOM.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Total - Number',
            'JPN.TTL.SDDOM.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Domestic/Breakdown by Issue Type); Total - Amount',
            
            # Secondary overseas
            'JPN.NIPO.SDOVR.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Secondary Distributions (Non IPO) - Number',
            'JPN.NIPO.SDOVR.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Secondary Distributions (Non IPO) - Amount',
            'JPN.IPO.SDOVR.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Secondary Distributions (IPO) - Number',
            'JPN.IPO.SDOVR.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Secondary Distributions (IPO) - Amount',
            'JPN.TTL.SDOVR.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Total Overseas Secondary Distributions - Number',
            'JPN.TTL.SDOVR.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distributions/Overseas/Breakdown by Issue Type); Total Overseas Secondary Distributions - Amount',
            
            # Secondary total
            'JPN.NIPO.SDTTL.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Stocks(Non IPO) - Number',
            'JPN.NIPO.SDTTL.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Stocks(Non IPO) - Amount',
            'JPN.IPO.SDTTL.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Stocks(IPO) - Number',
            'JPN.IPO.SDTTL.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Stocks(IPO) - Amount',
            'JPN.TTL.SDTTL.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Total - Number',
            'JPN.TTL.SDTTL.A.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Type); Total - Amount',
            
            # Secondary size
            'JPN.L10B.SDSZ.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Size); Less than 10 billion yen - Number',
            'JPN.L50B.SDSZ.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Size); 10 billion yen to less than 50 billion yen - Number',
            'JPN.L100B.SDSZ.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Size); 50 billion yen to less than 100 billion yen - Number',
            'JPN.M100B.SDSZ.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Size); 100 billion yen or more - Number',
            'JPN.TTL.SDSZ.N.M': 'Equity Financing by Companies Listed in Japan (Secondary Distribution/Total/Breakdown by Issue Size); Total - Number'
        }

    def _setup_sheet_mappings(self):
        """Map Japanese sheet names to categories and indices."""
        self.sheet_mappings = {
            '募集国内': ('domestic', 0),
            '募集海外': ('overseas', 1), 
            '募集合計': ('total', 2),
            '募集規模内訳': ('size_breakdown', 3),
            '海外内訳': ('market_breakdown', 4),
            '参考': ('reference', 5),
            '売出国内': ('secondary_domestic', 6),
            '売出海外': ('secondary_overseas', 7),
            '売出合計': ('secondary_total', 8),
            '売出規模内訳': ('secondary_size', 9)
        }

    def _detect_columns_dynamically(self, df: pd.DataFrame, sheet_type: str) -> Tuple[int, Dict[int, str]]:
        """Detect column mappings dynamically by scanning headers."""
        logger.info(f"Detecting columns dynamically for sheet type: {sheet_type}")

        # Get indicators from config
        number_indicators = self.column_indicators.get('number', ['件数', 'Number'])
        amount_indicators = self.column_indicators.get('amount', ['調達額', '売出額', 'Amount'])
        all_indicators = number_indicators + amount_indicators

        # Get year indicators from config for date detection
        year_indicators = self.config.get('date_patterns', {}).get('year_indicators',
                                                                   ['2020', '2021', '2022', '2023', '2024', '2025', '2026'])

        # Find header row (contains Number indicators)
        header_row = -1
        for row_idx in range(min(15, len(df))):
            row_text = ' '.join([str(v) if pd.notna(v) else '' for v in df.iloc[row_idx]])
            if any(ind in row_text for ind in number_indicators):
                header_row = row_idx
                break

        if header_row == -1:
            logger.warning(f"Could not find header row for {sheet_type}")
            return self._get_fallback_mapping(sheet_type)

        # Find data start row (first row with date after header)
        data_start_row = -1
        for row_idx in range(header_row + 1, len(df)):
            first_cell = df.iloc[row_idx, 0]
            if pd.notna(first_cell):
                if 'datetime' in str(type(first_cell)).lower():
                    data_start_row = row_idx
                    break
                elif any(y in str(first_cell) for y in year_indicators):
                    data_start_row = row_idx
                    break

        if data_start_row == -1:
            data_start_row = header_row + 2

        # Verify this row actually has numeric data (not just a date)
        while data_start_row < len(df):
            row_data = df.iloc[data_start_row, 1:]
            has_numeric_data = False
            for val in row_data:
                if pd.notna(val):
                    try:
                        if isinstance(val, (int, float)) and val != 0:
                            has_numeric_data = True
                            break
                        elif isinstance(val, str):
                            clean = re.sub(r'[^\d.-]', '', str(val))
                            if clean and clean != '-' and float(clean) != 0:
                                has_numeric_data = True
                                break
                    except (ValueError, TypeError):
                        continue
            if has_numeric_data:
                break
            data_start_row += 1

        # Get scope for this sheet type
        scope = self.sheet_scope.get(sheet_type, 'DOM')

        # Build column mapping by scanning headers
        col_mapping = {}

        # For size breakdown sheets, columns are just numbers (no Number/Amount distinction)
        is_size_sheet = sheet_type in ['size_breakdown', 'secondary_size']

        # Track current category as we scan left-to-right (categories span multiple columns)
        current_category = None
        current_targets = (None, None)
        pending_amount_target = None  # Track when we need to map next column as amount

        # Collect all header text for each column (scan multiple rows above header_row)
        for col_idx in range(1, len(df.columns)):  # Skip column 0 (date column)
            header_texts = []
            for scan_row in range(max(0, header_row - 6), header_row + 1):
                if scan_row < len(df):
                    val = df.iloc[scan_row, col_idx]
                    if pd.notna(val):
                        header_texts.append(str(val).strip())

            combined_header = ' '.join(header_texts)

            # If previous column had Amount header but no data, this column has the amount value
            # Check if header is empty OR only contains non-data headers (like unit labels)
            is_data_column = True
            if combined_header:
                # Only consider as non-data if it contains actual category or indicator headers
                has_category = any(pattern in combined_header for pattern, _ in self.header_to_target.items() if pattern not in all_indicators)
                has_indicator = any(ind in combined_header for ind in all_indicators)
                is_data_column = not has_category and not has_indicator

            if pending_amount_target and is_data_column:
                # Check if this column has numeric data
                sample_val = df.iloc[data_start_row, col_idx] if data_start_row < len(df) else None
                if pd.notna(sample_val) and (isinstance(sample_val, (int, float)) or str(sample_val).replace('.','').replace('-','').isdigit()):
                    col_mapping[col_idx] = pending_amount_target
                    pending_amount_target = None
                    continue

            pending_amount_target = None  # Reset if not used

            # Check if this column has a category header (not just Number/Amount)
            # Sort patterns by length (longest first) to match most specific pattern
            category_found = False
            sorted_patterns = sorted(self.header_to_target.items(), key=lambda x: len(x[0]), reverse=True)
            for pattern, targets in sorted_patterns:
                if pattern in combined_header:
                    # Check this isn't just a Number/Amount match
                    if pattern not in all_indicators:
                        current_category = pattern
                        current_targets = targets
                        category_found = True
                        break

            # Determine if this is a Number or Amount column
            is_number_col = any(ind in combined_header for ind in number_indicators)
            is_amount_col = any(ind in combined_header for ind in amount_indicators)

            # Map the column using current category context
            if current_targets[0]:
                if is_size_sheet:
                    # Size sheets - match category directly
                    if category_found:
                        target = current_targets[0].format(scope=scope) if '{scope}' in current_targets[0] else current_targets[0]
                        col_mapping[col_idx] = target
                elif is_number_col:
                    target = current_targets[0].format(scope=scope) if '{scope}' in current_targets[0] else current_targets[0]
                    col_mapping[col_idx] = target
                elif is_amount_col and current_targets[1]:
                    target = current_targets[1].format(scope=scope) if '{scope}' in current_targets[1] else current_targets[1]
                    # Check if this column actually has data, or if data is in next column
                    sample_val = df.iloc[data_start_row, col_idx] if data_start_row < len(df) else None
                    if pd.notna(sample_val) and (isinstance(sample_val, (int, float)) or str(sample_val).replace('.','').replace('-','').isdigit()):
                        col_mapping[col_idx] = target
                    else:
                        # Amount data is likely in the next column (merged cell pattern)
                        pending_amount_target = target

        # Get expected column count from fallback mapping
        fallback_mapping = self.final_mappings.get(sheet_type, {})
        expected_cols = len(fallback_mapping)

        # Use dynamic detection if it found enough columns, otherwise use fallback
        if col_mapping and len(col_mapping) >= expected_cols * 0.8:
            logger.info(f"Dynamic detection found {len(col_mapping)}/{expected_cols} columns for {sheet_type}")
            return data_start_row, col_mapping
        elif col_mapping:
            # Merge dynamic with fallback - dynamic takes precedence for matched columns
            merged = fallback_mapping.copy()
            for col_idx, target in col_mapping.items():
                merged[col_idx] = target
            logger.info(f"Merged mapping: dynamic {len(col_mapping)} + fallback, total {len(merged)} for {sheet_type}")
            return data_start_row, merged
        else:
            logger.info(f"Using fallback mapping for {sheet_type}")
            return data_start_row, fallback_mapping

    def _get_fallback_mapping(self, sheet_type: str) -> Tuple[int, Dict[int, str]]:
        """Return fallback hardcoded mapping."""
        if sheet_type in ['overseas', 'market_breakdown']:
            data_start = 14
        else:
            data_start = 13
        return data_start, self.final_mappings.get(sheet_type, {})

    def _setup_final_mappings(self):
        """Final column mapping with corrected ranges and comprehensive coverage."""
        
        self.final_mappings = {
            # Primary sheets - Domestic (19 columns, all work)
            'domestic': {
                1: 'JPN.SNIPO.DOM.N.M',   # Stocks Non-IPO Number
                3: 'JPN.SNIPO.DOM.A.M',   # Stocks Non-IPO Amount (offset +2)
                4: 'JPN.CB.DOM.N.M',      # Convertible Bonds Number
                6: 'JPN.CB.DOM.A.M',      # Convertible Bonds Amount (offset +2)
                7: 'JPN.BWSO.DOM.N.M',    # Bond with Stock Options Number
                9: 'JPN.BWSO.DOM.A.M',    # Bond with Stock Options Amount (offset +2)
                10: 'JPN.SOC.DOM.N.M',    # Stock Option Certificates Number
                12: 'JPN.SOC.DOM.A.M',    # Stock Option Certificates Amount (offset +2)
                13: 'JPN.SIPO.DOM.N.M',   # Stocks with IPO Number
                15: 'JPN.SIPO.DOM.A.M',   # Stocks with IPO Amount (offset +2)
                16: 'JPN.TF.DOM.N.M',     # Trust Fund Number  
                18: 'JPN.TF.DOM.A.M',     # Trust Fund Amount (offset +2)
            },
            
            # Overseas (16 columns, CORRECTED mapping - TF and SIPO were swapped)
            'overseas': {
                1: 'JPN.SNIPO.OVR.N.M',   # Stocks Non-IPO Number
                3: 'JPN.SNIPO.OVR.A.M',   # Stocks Non-IPO Amount (offset +2)
                4: 'JPN.CB.OVR.N.M',      # Convertible Bonds Number
                6: 'JPN.CB.OVR.A.M',      # Convertible Bonds Amount (offset +2)
                7: 'JPN.BWSO.OVR.N.M',    # Bond with Stock Options Number
                9: 'JPN.BWSO.OVR.A.M',    # Bond with Stock Options Amount (offset +2)
                10: 'JPN.SIPO.OVR.N.M',   # Stocks with IPO Number (CORRECTED from 13)
                12: 'JPN.SIPO.OVR.A.M',   # Stocks with IPO Amount (CORRECTED from 15)
                13: 'JPN.TF.OVR.N.M',     # Trust Fund Number (CORRECTED from 10)
                15: 'JPN.TF.OVR.A.M',     # Trust Fund Amount (CORRECTED from 12)
            },
            
            # Total (19 columns, all work)
            'total': {
                1: 'JPN.SNIPO.TTL.N.M',   # Stocks Non-IPO Number
                3: 'JPN.SNIPO.TTL.A.M',   # Stocks Non-IPO Amount (offset +2)
                4: 'JPN.CB.TTL.N.M',      # Convertible Bonds Number
                6: 'JPN.CB.TTL.A.M',      # Convertible Bonds Amount (offset +2)
                7: 'JPN.BWSO.TTL.N.M',    # Bond with Stock Options Number
                9: 'JPN.BWSO.TTL.A.M',    # Bond with Stock Options Amount (offset +2)
                10: 'JPN.SOC.TTL.N.M',    # Stock Option Certificates Number
                12: 'JPN.SOC.TTL.A.M',    # Stock Option Certificates Amount (offset +2)
                13: 'JPN.SIPO.TTL.N.M',   # Stocks with IPO Number
                15: 'JPN.SIPO.TTL.A.M',   # Stocks with IPO Amount (offset +2)
                16: 'JPN.TF.TTL.N.M',     # Trust Fund Number
                18: 'JPN.TF.TTL.A.M',     # Trust Fund Amount (offset +2)
            },
            
            # Size breakdown (6 columns) - Add missing L100B
            'size_breakdown': {
                1: 'JPN.L10B.SZ.N.M',     # Less than 10 billion yen
                2: 'JPN.L50B.SZ.N.M',     # 10-50 billion yen  
                3: 'JPN.L100B.SZ.N.M',    # 50-100 billion yen
                4: 'JPN.M100B.SZ.N.M',    # More than 100 billion yen
                5: 'JPN.TTL.SZ.N.M',      # Total
            },
            
            # Market breakdown (7 columns) - Adjust OTH to fit range
            'market_breakdown': {
                1: 'JPN.SF.MRKT.N.M',      # Swiss Franc Number
                2: 'JPN.SF.MRKT.A.M',      # Swiss Franc Amount (adjacent instead of offset)
                3: 'JPN.EURL.MRKT.N.M',    # Eurodollar Number (adjusted from 4)
                4: 'JPN.EURL.MRKT.A.M',    # Eurodollar Amount (adjusted from 6)
                5: 'JPN.OTH.MRKT.N.M',     # Other Markets Number (adjusted from 7)
                6: 'JPN.OTH.MRKT.A.M',     # Other Markets Amount (adjusted from 9)
            },
            
            # Reference (7 columns) - Adjust SOC to fit range
            'reference': {
                1: 'JPN.SH.REF.N.M',       # Shareholder Allotment Number
                2: 'JPN.SH.REF.A.M',       # Shareholder Allotment Amount (adjacent)
                3: 'JPN.TP.REF.N.M',       # Third Party Allotment Number (adjusted from 4)
                4: 'JPN.TP.REF.A.M',       # Third Party Allotment Amount (adjusted from 6)
                5: 'JPN.SOC.REF.N.M',      # Share Option Certificates Number (adjusted from 7)
                6: 'JPN.SOC.REF.A.M',      # Share Option Certificates Amount (adjusted from 9)
            },
            
            # Secondary domestic (10 columns, all work)
            'secondary_domestic': {
                1: 'JPN.NIPO.SDDOM.N.M',   # Non-IPO Secondary Domestic Number
                3: 'JPN.NIPO.SDDOM.A.M',   # Non-IPO Secondary Domestic Amount (offset +2)
                4: 'JPN.IPO.SDDOM.N.M',    # IPO Secondary Domestic Number
                6: 'JPN.IPO.SDDOM.A.M',    # IPO Secondary Domestic Amount (offset +2)
                7: 'JPN.TTL.SDDOM.N.M',    # Total Secondary Domestic Number
                9: 'JPN.TTL.SDDOM.A.M',    # Total Secondary Domestic Amount (offset +2)
            },
            
            # Secondary overseas (10 columns, all work)
            'secondary_overseas': {
                1: 'JPN.NIPO.SDOVR.N.M',   # Non-IPO Secondary Overseas Number
                3: 'JPN.NIPO.SDOVR.A.M',   # Non-IPO Secondary Overseas Amount (offset +2)
                4: 'JPN.IPO.SDOVR.N.M',    # IPO Secondary Overseas Number
                6: 'JPN.IPO.SDOVR.A.M',    # IPO Secondary Overseas Amount (offset +2)
                7: 'JPN.TTL.SDOVR.N.M',    # Total Secondary Overseas Number
                9: 'JPN.TTL.SDOVR.A.M',    # Total Secondary Overseas Amount (offset +2)
            },
            
            # Secondary total (10 columns, all work)
            'secondary_total': {
                1: 'JPN.NIPO.SDTTL.N.M',   # Non-IPO Secondary Total Number
                3: 'JPN.NIPO.SDTTL.A.M',   # Non-IPO Secondary Total Amount (offset +2)
                4: 'JPN.IPO.SDTTL.N.M',    # IPO Secondary Total Number
                6: 'JPN.IPO.SDTTL.A.M',    # IPO Secondary Total Amount (offset +2)
                7: 'JPN.TTL.SDTTL.N.M',    # Total Secondary Total Number
                9: 'JPN.TTL.SDTTL.A.M',    # Total Secondary Total Amount (offset +2)
            },
            
            # Secondary size breakdown (6 columns) - Add missing L100B
            'secondary_size': {
                1: 'JPN.L10B.SDSZ.N.M',    # Less than 10B Secondary Size
                2: 'JPN.L50B.SDSZ.N.M',    # 10-50B Secondary Size
                3: 'JPN.L100B.SDSZ.N.M',   # 50-100B Secondary Size  
                4: 'JPN.M100B.SDSZ.N.M',   # More than 100B Secondary Size
                5: 'JPN.TTL.SDSZ.N.M',     # Total Secondary Size
            }
        }

    def process_sheet(self, df: pd.DataFrame, sheet_type: str, sheet_name: str) -> Dict[str, float]:
        """Process a sheet with dynamic column detection."""
        data_dict = {}

        # Use dynamic column detection
        data_row_start, mapping = self._detect_columns_dynamically(df, sheet_type)

        if not mapping:
            logger.warning(f"No mapping found for sheet type: {sheet_type}")
            return data_dict

        logger.info(f"Processing {sheet_type} sheet with {len(mapping)} mappings, data starts at row {data_row_start}")
        total_updated = 0
        
        # Process all available months dynamically
        month = 0
        while True:
            data_row = data_row_start + month
            
            # Stop if we've reached the end of available data
            if data_row >= len(df):
                logger.info(f"Sheet {sheet_name} processed {month} months of data")
                break
            
            # Check if this row has any meaningful data (not just zeros/empty)
            row_data = df.iloc[data_row, 1:]  # Skip first column (usually date)
            has_data = any(pd.notna(val) and val != 0 and str(val).strip() != '' for val in row_data)

            # Also check if row has a valid date - if no date, we've reached end of data
            first_cell = df.iloc[data_row, 0]
            has_date = pd.notna(first_cell) and (
                hasattr(first_cell, 'year') or
                any(y in str(first_cell) for y in ['2025', '2024', '2023', '2022', '2021', '2020'])
            )

            if not has_date:
                logger.info(f"Sheet {sheet_name} processed {month} months of data (no more dates)")
                break

            if not has_data:
                # Skip this month but continue processing (month may have zero data)
                logger.info(f"Sheet {sheet_name} month {month+1}: No data, skipping")
                month += 1
                continue
            
            monthly_data = {}
            month_updated = 0
            
            for col_idx, target_col in mapping.items():
                if col_idx < len(df.columns):
                    try:
                        cell_value = df.iloc[data_row, col_idx]
                        
                        if pd.notna(cell_value):
                            # Convert to numeric, handling various formats
                            if isinstance(cell_value, str):
                                # Clean string: remove non-numeric chars except decimal points
                                clean_value = re.sub(r'[^\d.-]', '', str(cell_value))
                                if clean_value and clean_value != '-':
                                    numeric_value = float(clean_value)
                                else:
                                    numeric_value = 0.0
                            else:
                                numeric_value = float(cell_value)
                            
                            monthly_data[target_col] = numeric_value
                            month_updated += 1
                            
                            logger.debug(f"  Col {col_idx:2d} -> {target_col}: {numeric_value}")
                        else:
                            monthly_data[target_col] = 0.0
                    except Exception as e:
                        logger.warning(f"Error processing {sheet_name} col {col_idx}: {e}")
                        monthly_data[target_col] = 0.0
                else:
                    logger.warning(f"Column {col_idx} out of range for {sheet_name} (has {len(df.columns)} cols)")
                    monthly_data[target_col] = 0.0
            
            # Extract date from first column dynamically
            first_cell = df.iloc[data_row, 0]
            month_label = None

            if pd.notna(first_cell):
                # Handle datetime objects
                if hasattr(first_cell, 'year') and hasattr(first_cell, 'month'):
                    month_label = f"{first_cell.year}-{first_cell.month:02d}"
                else:
                    # Try to parse string dates
                    cell_str = str(first_cell)
                    # Look for year-month patterns
                    date_match = re.search(r'(\d{4})[年/-]?\s*(\d{1,2})', cell_str)
                    if date_match:
                        year = int(date_match.group(1))
                        month_num = int(date_match.group(2))
                        month_label = f"{year}-{month_num:02d}"
                    else:
                        # Fallback: use month counter
                        month_label = f"2025-{month+1:02d}"
            else:
                month_label = f"2025-{month+1:02d}"

            data_dict[month_label] = monthly_data
            total_updated += month_updated
            month += 1  # Increment month counter
            
            if month_updated > 0:
                logger.info(f"  Month {month:2d}: Updated {month_updated} columns")
        
        logger.info(f"Sheet {sheet_type}: Total {total_updated} column updates across all months")
        return data_dict

    def create_output_dataframe(self, all_data: Dict) -> pd.DataFrame:
        """Create the final output DataFrame with all 74 columns using JSDA_DATA header format."""
        logger.info("Creating final output DataFrame...")
        
        # Create header rows matching JSDA_DATA format
        # Row 1: Column names
        header_row1 = [None] + self.target_columns
        # Row 2: Descriptions (keep consistent with original JSDA_DATA format)
        header_row2 = [None] + [self.column_descriptions[col] for col in self.target_columns]
        
        # Create data rows 
        data_rows = []
        total_values = 0
        
        # Get all available months dynamically from the data
        all_months = set()
        for sheet_data in all_data.values():
            all_months.update(sheet_data.keys())

        # Sort months to ensure proper order
        sorted_months = sorted(list(all_months))
        logger.info(f"Processing {len(sorted_months)} months with data: {sorted_months}")
        
        for date_key in sorted_months:
            row_data = [date_key]
            row_values = 0

            for target_col in self.target_columns:
                col_value = 0.0

                # Sum values from all sheets for this column and month
                for sheet_data in all_data.values():
                    if date_key in sheet_data and target_col in sheet_data[date_key]:
                        col_value += sheet_data[date_key][target_col]

                row_data.append(col_value)
                if col_value != 0:
                    row_values += 1
                    total_values += 1

            # Skip rows with no data (all zeros)
            if row_values == 0:
                logger.info(f"  {date_key}: Skipping - no data")
                continue

            data_rows.append(row_data)
            logger.info(f"  {date_key}: {row_values} non-zero columns")
        
        # Combine all rows
        all_rows = [header_row1, header_row2] + data_rows
        
        # Create DataFrame
        df = pd.DataFrame(all_rows)
        
        # Calculate coverage statistics
        numeric_rows = len(sorted_months)  # Use actual number of months with data
        numeric_cols = len(self.target_columns)
        total_cells = numeric_rows * numeric_cols
        coverage_pct = (total_values / total_cells * 100) if total_cells > 0 else 0
        
        # Count columns with data
        cols_with_data = 0
        for col_idx, target_col in enumerate(self.target_columns):
            col_has_data = any(row[col_idx + 1] != 0 for row in data_rows)
            if col_has_data:
                cols_with_data += 1
        
        logger.info(f"Final DataFrame: {numeric_rows} rows x {numeric_cols + 1} columns")
        logger.info(f"Data coverage: {coverage_pct:.1f}% ({total_values:,} non-zero values)")
        logger.info(f"Columns with data: {cols_with_data}/{numeric_cols} ({cols_with_data/numeric_cols*100:.1f}%)")
        
        return df

    def process_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the Excel file and return the final DataFrame."""
        logger.info(f"Processing Excel file: {file_path}")
        
        if not Path(file_path).exists():
            logger.error(f"File not found: {file_path}")
            return None
        
        all_data = {}
        sheets_processed = 0
        
        try:
            # Read all sheets and get their names
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            logger.info(f"Found {len(sheet_names)} sheets in Excel file")
            
            # Process each known sheet by index
            for sheet_index in range(min(10, len(sheet_names))):
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_index, header=None)
                    sheet_name = sheet_names[sheet_index]
                    
                    logger.info(f"Processing sheet {sheet_index}: '{sheet_name}' ({len(df)} rows x {len(df.columns)} cols)")
                    
                    # Find matching sheet type
                    sheet_type = None
                    for jp_name, (eng_name, idx) in self.sheet_mappings.items():
                        if idx == sheet_index:
                            sheet_type = eng_name
                            break
                    
                    if sheet_type:
                        sheet_data = self.process_sheet(df, sheet_type, sheet_name)
                        if sheet_data:
                            all_data[sheet_type] = sheet_data
                            sheets_processed += 1
                            logger.info(f"Successfully processed sheet: {sheet_type}")
                        else:
                            logger.warning(f"No data extracted from sheet: {sheet_type}")
                    else:
                        logger.info(f"Skipping unmapped sheet {sheet_index}: {sheet_name}")
                        
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_index}: {e}")
                    continue
            
            logger.info(f"Total sheets processed successfully: {sheets_processed}")
            
            if sheets_processed == 0:
                logger.error("No sheets were processed successfully")
                return None
                
            # Create final output
            return self.create_output_dataframe(all_data)
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            return None

    def save_output(self, df: pd.DataFrame, output_dir: str = "JSDA_FINAL_OUTPUT") -> Optional[str]:
        """Save the processed data to Excel file."""
        if df is None:
            logger.error("No data to save")
            return None
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"JSDA_FINAL_{timestamp}.xlsx"
        output_file = output_path / filename
        
        try:
            # Save to Excel without headers/index
            df.to_excel(output_file, index=False, header=False)
            
            logger.info(f"Output saved successfully: {output_file}")
            logger.info(f"Log file: {log_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error saving output: {e}")
            return None

def main():
    """Main processing function."""
    print("=" * 80)
    print("JSDA FINAL DATA PROCESSOR")
    print("=" * 80)
    
    processor = FinalJSDAProcessor()
    
    # Process the Excel file
    input_file = "zoushi2025-1.xls"
    result_df = processor.process_excel_file(input_file)
    
    if result_df is not None:
        # Save output
        output_file = processor.save_output(result_df)
        
        if output_file:
            print(f"\nProcessing completed successfully!")
            print(f"Output file: {output_file}")
            print(f"Log file: {log_file}")
            print("\nNext steps:")
            print("1. Run compare_data.py to validate results")
            print("2. Check coverage statistics in the log file")
        else:
            print("Error saving output file")
    else:
        print("Processing failed - check log file for details")

if __name__ == "__main__":
    main()