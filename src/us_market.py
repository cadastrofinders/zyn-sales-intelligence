#!/usr/bin/env python3
"""
US Market Intelligence — Processamento e análise de investidores
americanos com exposição ao Brasil, baseado em dados SEC/EDGAR.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

from src.sec_edgar import (
    collect_brazil_holdings,
    build_us_investor_profiles,
)

logger = logging.getLogger(__name__)


def load_us_holdings(data_dir: Path) -> pd.DataFrame:
    """Load cached US holdings data."""
    path = data_dir / "us_holdings_brazil.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def load_us_profiles(data_dir: Path) -> pd.DataFrame:
    """Load cached US investor profiles."""
    path = data_dir / "us_investor_profiles.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def refresh_us_data(data_dir: Path, max_managers: int = 50,
                    progress_callback=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full refresh: download from SEC, parse, build profiles.
    Returns (holdings_df, profiles_df).
    """
    holdings = collect_brazil_holdings(
        output_dir=data_dir,
        max_managers=max_managers,
        filings_per_manager=1,
        progress_callback=progress_callback,
    )

    if holdings.empty:
        return holdings, pd.DataFrame()

    profiles = build_us_investor_profiles(holdings, data_dir)
    return holdings, profiles


def match_us_investors_to_deal(
    profiles_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    deal_sector: str = "",
    deal_type: str = "",  # "CRI", "CRA", "Debênture", etc.
    deal_amount_usd: float = 0,
    deal_issuer: str = "",
) -> pd.DataFrame:
    """
    Match US investors to a specific deal based on their Brazil exposure profile.

    Scoring:
    - Volume Brasil: higher = more active
    - % Corporativo: higher = more likely for corporate deals
    - Sector overlap: if they already hold similar issuers
    """
    if profiles_df.empty:
        return pd.DataFrame()

    df = profiles_df.copy()

    # Base score: volume-weighted (log scale)
    import numpy as np
    df["score_volume"] = np.log10(df["Vol. Brasil (USD)"].clip(lower=1)) / 10

    # Corporate preference score
    df["score_corporate"] = df["% Corporativo"] / 100

    # Sector overlap (check if deal issuer or sector appears in top emissores)
    if deal_issuer:
        df["score_sector"] = df["Top Emissores BR"].str.contains(
            deal_issuer, case=False, na=False
        ).astype(float) * 0.3
    else:
        df["score_sector"] = 0

    # Size fit: penalize if deal is too large relative to their Brazil book
    if deal_amount_usd > 0:
        df["deal_pct"] = deal_amount_usd / df["Vol. Brasil (USD)"].clip(lower=1)
        df["score_size"] = 1 - df["deal_pct"].clip(upper=1)
    else:
        df["score_size"] = 0.5

    # Combined score
    df["Match Score"] = (
        df["score_volume"] * 0.4
        + df["score_corporate"] * 0.3
        + df["score_sector"] * 0.15
        + df["score_size"] * 0.15
    )

    # Normalize to 0-100
    max_score = df["Match Score"].max()
    if max_score > 0:
        df["Match Score"] = (df["Match Score"] / max_score * 100).round(1)

    return (
        df[["Manager", "CIK", "Vol. Brasil (USD)", "% Corporativo",
            "Nº Posições BR", "Top Emissores BR", "Match Score"]]
        .sort_values("Match Score", ascending=False)
    )


def us_market_summary(holdings_df: pd.DataFrame, profiles_df: pd.DataFrame) -> dict:
    """Generate summary statistics for the US Market overview."""
    if holdings_df.empty:
        return {
            "total_managers": 0,
            "total_positions": 0,
            "total_volume_usd": 0,
            "vol_sovereign": 0,
            "vol_corporate": 0,
            "top_managers": [],
            "top_issuers": [],
            "asset_breakdown": {},
        }

    return {
        "total_managers": profiles_df["Manager"].nunique() if not profiles_df.empty else 0,
        "total_positions": len(holdings_df),
        "total_volume_usd": holdings_df["val_usd"].sum(),
        "vol_sovereign": profiles_df["Vol. Soberano (USD)"].sum() if not profiles_df.empty else 0,
        "vol_corporate": profiles_df["Vol. Corporativo (USD)"].sum() if not profiles_df.empty else 0,
        "top_managers": (
            profiles_df.nlargest(10, "Vol. Brasil (USD)")[["Manager", "Vol. Brasil (USD)"]]
            .to_dict("records")
            if not profiles_df.empty else []
        ),
        "top_issuers": (
            holdings_df.groupby("name")["val_usd"]
            .sum()
            .sort_values(ascending=False)
            .head(15)
            .to_dict()
        ),
        "asset_breakdown": holdings_df["asset_cat"].value_counts().to_dict(),
    }
