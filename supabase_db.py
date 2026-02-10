# -*- coding: utf-8 -*-
import logging
import os
from typing import Dict, Any, Optional, List
from supabase import create_client, Client
from datetime import datetime, timedelta

# Environment-based configuration with hardcoded fallbacks
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://jubcotqsbvguwzklngzd.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp1YmNvdHFzYnZndXd6a2xuZ3pkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTU0MjA3MCwiZXhwIjoyMDc1MTE4MDcwfQ.1HV-o9JFa_nCZGXcoap2OgOCKjRSlyFSRvKmYk70eDk")

logger = logging.getLogger(__name__)

class SupabaseService:
    def __init__(self):
        self.supabase: Optional[Client] = None
        try:
            # logger.info(f"Connecting to {SUPABASE_URL} with key {SUPABASE_KEY[:10]}...")
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Supabase client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")

    async def log_entry_generation(self, signal_data: Dict[str, Any]):
        """Logs a trade signal to 'entry_generation' table."""
        if not self.supabase: return None
        try:
            now = datetime.utcnow()
            valid_until = now + timedelta(hours=1)
            entry_data = {
                "strategy_id": "daily_scan_v1",
                "symbol": signal_data.get("symbol", "UNKNOWN"),
                "direction": signal_data.get("direction", "none").upper(),
                "generated_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "entry_price": signal_data.get("entry_price", 0.0),
                "stop_loss_price": signal_data.get("stop_loss", 0.0),
                "take_profit_price": signal_data.get("take_profit", 0.0),
                "confidence_score": signal_data.get("confidence", 0.0),
                "status": signal_data.get("status", "PENDING")
            }
            response = self.supabase.table("entry_generation").insert(entry_data).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error logging entry_generation: {e}")
            return None

    async def log_signal(self, signal_data: Dict[str, Any]):
        """
        Logs a generated signal to 'ml_signals'.
        Replaces log_entry_generation.
        """
        if not self.supabase: return None
        try:
            # Prepare metadata
            meta = {
                "audit_trail": signal_data.get("audit_trail"),
                "raw_scores": {
                    "p3_long": signal_data.get("p3_long"),
                    "p3_short": signal_data.get("p3_short"),
                    "atr_20": signal_data.get("atr_20")
                }
            }

            data = {
                "model_id": signal_data.get("model_id"), # Optional if we have a registry
                "symbol": signal_data.get("symbol"),
                "direction": signal_data.get("direction").upper(),
                "confidence": signal_data.get("confidence"),
                "price_at_signal": signal_data.get("entry_price"),
                "sl": signal_data.get("stop_loss_price"),
                "tp": signal_data.get("take_profit_price"),
                "regime_gauge": signal_data.get("regime_gauge"),
                "meta": meta,
                "status": "PENDING"
            }
            response = self.supabase.table("ml_signals").insert(data).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error logging to ml_signals: {e}")
            return None

    def fetch_pending_signals(self) -> List[Dict[str, Any]]:
        """Fetches signals with status='PENDING'."""
        if not self.supabase: return []
        try:
            resp = self.supabase.table("ml_signals")\
                .select("*")\
                .eq("status", "PENDING")\
                .order("created_at", desc=True)\
                .execute()
            return resp.data
        except Exception as e:
            logger.error(f"Error fetching pending signals: {e}")
            return []

    def create_trade_from_signal(self, signal_id: str, entry_price: float, entry_time: str) -> Optional[Dict[str, Any]]:
        """
        Creates a new trade in 'ml_trades' and updates signal status to 'EXECUTED'.
        """
        if not self.supabase: return None
        try:
            # 1. Create trade
            trade_data = {
                "signal_id": signal_id,
                "entry_time": entry_time,
                "entry_price": entry_price,
                "status": "OPEN"
            }
            trade_resp = self.supabase.table("ml_trades").insert(trade_data).execute()
            
            if trade_resp.data:
                # 2. Update signal status
                self.supabase.table("ml_signals").update({"status": "EXECUTED"}).eq("id", signal_id).execute()
                return trade_resp.data[0]
            return None
        except Exception as e:
            logger.error(f"Error creating trade from signal: {e}")
            return None

    def close_trade(self, trade_id: str, exit_price: float, exit_time: str, exit_reason: str = "MANUAL"):
        """
        Closes a trade in 'ml_trades', calculating PnL.
        """
        if not self.supabase: return None
        try:
            # Fetch trade to get entry details
            trade = self.supabase.table("ml_trades").select("*, ml_signals(direction)").eq("id", trade_id).single().execute()
            if not trade.data:
                return None
            
            t_data = trade.data
            direction = t_data.get("ml_signals", {}).get("direction", "LONG") # Default LONG if join fails
            entry_price = t_data["entry_price"]

            # Calculate PnL
            if direction == "LONG":
                pnl_usd = exit_price - entry_price
                pnl_pct = (exit_price - entry_price) / entry_price
            else:
                pnl_usd = entry_price - exit_price
                pnl_pct = (entry_price - exit_price) / entry_price

            update_data = {
                "exit_price": exit_price,
                "exit_time": exit_time,
                "pnl_usd": pnl_usd,
                "pnl_pct": pnl_pct,
                "exit_reason": exit_reason,
                "status": "CLOSED"
            }
            
            resp = self.supabase.table("ml_trades").update(update_data).eq("id", trade_id).execute()
            return resp.data
        except Exception as e:
            logger.error(f"Error closing trade: {e}")
            return None

    def fetch_open_trades(self) -> List[Dict[str, Any]]:
        """Fetches trades with status='OPEN'."""
        if not self.supabase: return []
        try:
            # Join with signals to get symbol and direction
            resp = self.supabase.table("ml_trades")\
                .select("*, ml_signals(symbol, direction, confidence)")\
                .eq("status", "OPEN")\
                .order("created_at", desc=True)\
                .execute()
            
            # Flatten for easier consumption
            results = []
            for item in resp.data:
                sig = item.pop("ml_signals", {}) or {}
                item["symbol"] = sig.get("symbol")
                item["direction"] = sig.get("direction")
                item["confidence"] = sig.get("confidence")
                results.append(item)
            return results
        except Exception as e:
            logger.error(f"Error fetching open trades: {e}")
            return []

    # --- Legacy / Validation Support ---
    
    def fetch_recent_signals(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """Fetches signals from ml_signals for PSI calculation."""
        if not self.supabase: return []
        try:
            cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
            resp = self.supabase.table("ml_signals")\
                .select("id, symbol, direction, confidence, created_at")\
                .eq("symbol", symbol)\
                .gte("created_at", cutoff)\
                .order("created_at", desc=True)\
                .execute()
            return resp.data
        except Exception as e:
            logger.error(f"Error fetching signals from ml_signals: {e}")
            return []

    # --- Model Registry ---
    
    def register_model(self, name: str, version: str, config: Dict[str, Any] = {}) -> Optional[str]:
        """
        Registers a new model version in 'ml_models'.
        Returns the model UUID.
        """
        if not self.supabase: return None
        try:
            # Check if exists first? Or just insert new entry
            # If name+version unique constraint exists in DB, handle error
            # For now, let's assume we want to create a new entry every time or look up
            resp = self.supabase.table("ml_models")\
                .select("id")\
                .eq("name", name)\
                .eq("version", version)\
                .limit(1)\
                .execute()
                
            if resp.data:
                logger.info(f"Model {name} v{version} already registered.")
                return resp.data[0]["id"]
            
            # Insert new
            data = {
                "name": name,
                "version": version,
                "config_json": config,
                "is_active": True
            }
            res = self.supabase.table("ml_models").insert(data).execute()
            if res.data:
                logger.info(f"Registered model {name} v{version}")
                return res.data[0]["id"]
            return None
        except Exception as e:
            logger.error(f"Error registering model: {e}")
            return None

    def fetch_drift_baseline(self, symbol: str) -> Optional[Dict[str, Any]]:
        return None # Placeholder until we populate ml_drift_metrics

db_service = SupabaseService()
