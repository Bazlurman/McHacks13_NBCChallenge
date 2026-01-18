"""
High-Performance Trading Algorithm - Thread-Safe Market Maker
==============================================================
Features:
- Two-sided quoting (1 bid + 1 ask)
- Queue priority logic with aging system
- Adverse selection detection
- Microprice calculation
- Full thread-safety with locks
"""

import json
import websocket
import threading
import argparse
import time
import requests
import ssl
import urllib3
from typing import Dict, Optional, Tuple
from collections import deque
from dataclasses import dataclass
from enum import Enum

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Order:
    order_id: str
    side: OrderSide
    price: float
    qty: int
    step: int
    timestamp: float


@dataclass
class QuoteState:
    """Current active quotes"""
    bid_order: Optional[Order] = None
    ask_order: Optional[Order] = None


class TradingState:
    """Thread-safe trading state with locks"""
    
    def __init__(self):
        self.lock = threading.RLock()
        
        # Position tracking
        self.inventory = 0
        self.cash_flow = 0.0
        self.pnl = 0.0
        
        # Market data
        self.current_step = 0
        self.last_bid = 0.0
        self.last_ask = 0.0
        self.last_mid = 0.0
        self.last_bid_size = 0
        self.last_ask_size = 0
        
        # Active quotes
        self.quotes = QuoteState()
        
        # Queue priority tracking
        self.bid_stable_since = 0  # step when bid became top-of-book
        self.ask_stable_since = 0  # step when ask became top-of-book
        self.last_market_bid = 0.0
        self.last_market_ask = 0.0
        
        # History
        self.price_history = deque(maxlen=100)
        self.spread_history = deque(maxlen=50)
        self.fill_history = deque(maxlen=50)  # Recent fills with prices
        
        # Performance
        self.orders_sent = 0
        self.fills_received = 0
        
    def update_inventory(self, side: OrderSide, qty: int, price: float):
        """Thread-safe inventory update"""
        with self.lock:
            if side == OrderSide.BUY:
                self.inventory += qty
                self.cash_flow -= qty * price
            else:
                self.inventory -= qty
                self.cash_flow += qty * price
            
            self.fill_history.append((side, price, qty, time.time()))
            self.fills_received += 1
            self.pnl = self.cash_flow + self.inventory * self.last_mid
    
    def update_market(self, bid: float, ask: float, bid_size: int, ask_size: int, step: int):
        """Thread-safe market data update"""
        with self.lock:
            self.current_step = step
            self.last_bid = bid
            self.last_ask = ask
            self.last_bid_size = bid_size
            self.last_ask_size = ask_size
            
            if bid > 0 and ask > 0:
                self.last_mid = (bid + ask) / 2
                self.price_history.append(self.last_mid)
                self.spread_history.append(ask - bid)
            
            # Track queue priority
            if bid != self.last_market_bid:
                self.last_market_bid = bid
                self.bid_stable_since = step
            
            if ask != self.last_market_ask:
                self.last_market_ask = ask
                self.ask_stable_since = step
    
    def set_quote(self, side: OrderSide, order: Order):
        """Thread-safe quote update"""
        with self.lock:
            if side == OrderSide.BUY:
                self.quotes.bid_order = order
            else:
                self.quotes.ask_order = order
            self.orders_sent += 1
    
    def clear_quote(self, side: OrderSide):
        """Thread-safe quote removal"""
        with self.lock:
            if side == OrderSide.BUY:
                self.quotes.bid_order = None
            else:
                self.quotes.ask_order = None
    
    def get_snapshot(self) -> Dict:
        """Get thread-safe snapshot of current state"""
        with self.lock:
            return {
                'inventory': self.inventory,
                'pnl': self.pnl,
                'step': self.current_step,
                'bid': self.last_bid,
                'ask': self.last_ask,
                'mid': self.last_mid,
                'bid_size': self.last_bid_size,
                'ask_size': self.last_ask_size,
                'quotes': {
                    'bid': self.quotes.bid_order,
                    'ask': self.quotes.ask_order
                },
                'bid_stable_steps': self.current_step - self.bid_stable_since,
                'ask_stable_steps': self.current_step - self.ask_stable_since,
                'orders_sent': self.orders_sent,
                'fills': self.fills_received
            }


class SignalCalculator:
    """Calculate trading signals from market data"""
    
    @staticmethod
    def calculate_microprice(bid: float, ask: float, bid_size: int, ask_size: int) -> float:
        """
        Microprice: volume-weighted mid price
        More weight to side with more liquidity
        """
        if bid <= 0 or ask <= 0 or bid_size <= 0 or ask_size <= 0:
            return (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
        
        total_size = bid_size + ask_size
        return (bid * ask_size + ask * bid_size) / total_size
    
    @staticmethod
    def calculate_imbalance(bid_size: int, ask_size: int) -> float:
        """
        Order book imbalance: -1 (all asks) to +1 (all bids)
        Positive = buying pressure
        """
        total = bid_size + ask_size
        if total == 0:
            return 0.0
        return (bid_size - ask_size) / total
    
    @staticmethod
    def detect_adverse_selection(fill_history: deque, current_mid: float, window: int = 5) -> float:
        """
        Adverse selection score: how much did price move against us after fills?
        Positive = we're getting picked off (bad)
        """
        if len(fill_history) < 2 or current_mid <= 0:
            return 0.0
        
        recent_fills = list(fill_history)[-window:]
        adverse_score = 0.0
        
        for side, fill_price, qty, _ in recent_fills:
            if side == OrderSide.BUY:
                # We bought - adverse if price dropped after
                adverse_score += (fill_price - current_mid) / fill_price
            else:
                # We sold - adverse if price rose after
                adverse_score += (current_mid - fill_price) / fill_price
        
        return adverse_score / len(recent_fills) if recent_fills else 0.0


class TradingBot:
    """
    Advanced market maker with:
    - Two-sided quoting (1 bid, 1 ask)
    - Queue priority logic
    - Adverse selection detection
    - Thread-safe design
    """
    
    def __init__(self, student_id: str, host: str, scenario: str, password: str = None, secure: bool = False):
        self.student_id = student_id
        self.host = host
        self.scenario = scenario
        self.password = password
        self.secure = secure
        
        self.http_proto = "https" if secure else "http"
        self.ws_proto = "wss" if secure else "ws"
        
        self.token = None
        self.run_id = None
        
        # Thread-safe state
        self.state = TradingState()
        
        # Trading parameters
        self.MAX_INVENTORY = 1500
        self.EMERGENCY_INVENTORY = 2000
        self.MIN_EDGE = 0.02
        self.QUEUE_AGE_THRESHOLD = 3  # Keep order if at top for 3+ steps
        self.ADVERSE_SELECTION_THRESHOLD = 0.01  # Widen if getting picked off
        
        # WebSockets
        self.market_ws = None
        self.order_ws = None
        self.running = True
        
        # Separate order queue for thread safety
        self.order_queue = []
        self.order_queue_lock = threading.Lock()
        
    def register(self) -> bool:
        """Register with server"""
        print(f"[{self.student_id}] Registering for scenario '{self.scenario}'...")
        try:
            url = f"{self.http_proto}://{self.host}/api/replays/{self.scenario}/start"
            headers = {"Authorization": f"Bearer {self.student_id}"}
            if self.password:
                headers["X-Team-Password"] = self.password
            
            resp = requests.get(url, headers=headers, timeout=10, verify=not self.secure)
            
            if resp.status_code != 200:
                print(f"[{self.student_id}] Registration FAILED: {resp.text}")
                return False
            
            data = resp.json()
            self.token = data.get("token")
            self.run_id = data.get("run_id")
            
            if not self.token or not self.run_id:
                print(f"[{self.student_id}] Missing token or run_id")
                return False
            
            print(f"[{self.student_id}] Registered! Run ID: {self.run_id}")
            return True
        
        except Exception as e:
            print(f"[{self.student_id}] Registration error: {e}")
            return False
    
    def connect(self) -> bool:
        """Connect to WebSockets"""
        try:
            sslopt = {"cert_reqs": ssl.CERT_NONE} if self.secure else None
            
            market_url = f"{self.ws_proto}://{self.host}/api/ws/market?run_id={self.run_id}"
            self.market_ws = websocket.WebSocketApp(
                market_url,
                on_message=self._on_market_data,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Market data connected")
            )
            
            order_url = f"{self.ws_proto}://{self.host}/api/ws/orders?token={self.token}&run_id={self.run_id}"
            self.order_ws = websocket.WebSocketApp(
                order_url,
                on_message=self._on_order_response,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Order entry connected")
            )
            
            threading.Thread(target=lambda: self.market_ws.run_forever(sslopt=sslopt), daemon=True).start()
            threading.Thread(target=lambda: self.order_ws.run_forever(sslopt=sslopt), daemon=True).start()
            
            # Start order processing thread
            threading.Thread(target=self._process_order_queue, daemon=True).start()
            
            time.sleep(1)
            return True
        
        except Exception as e:
            print(f"[{self.student_id}] Connection error: {e}")
            return False
    
    def _process_order_queue(self):
        """Separate thread to process order queue"""
        while self.running:
            try:
                with self.order_queue_lock:
                    if self.order_queue and self.order_ws and self.order_ws.sock:
                        msg = self.order_queue.pop(0)
                        self.order_ws.send(json.dumps(msg))
                time.sleep(0.001)  # Small sleep to prevent busy waiting
            except Exception as e:
                print(f"[{self.student_id}] Order queue error: {e}")
    
    def _queue_order(self, msg: Dict):
        """Add order to queue (thread-safe)"""
        with self.order_queue_lock:
            self.order_queue.append(msg)
    
    def _on_market_data(self, ws, message: str):
        """Handle market data - ONLY reads and queues orders"""
        try:
            data = json.loads(message)
            
            if data.get("type") == "CONNECTED":
                return
            
            step = data.get("step", 0)
            bid = data.get("bid", 0.0)
            ask = data.get("ask", 0.0)
            bid_size = data.get("bid_size", 0)
            ask_size = data.get("ask_size", 0)
            
            # Thread-safe market update
            self.state.update_market(bid, ask, bid_size, ask_size, step)
            
            # Get snapshot for decision making
            snapshot = self.state.get_snapshot()
            
            # Periodic logging
            if step % 500 == 0:
                print(
                    f"[{self.student_id}] Step {step} | "
                    f"Inv: {snapshot['inventory']} | PnL: {snapshot['pnl']:.2f} | "
                    f"Orders: {snapshot['orders_sent']} | Fills: {snapshot['fills']}"
                )
            
            # Strategy execution
            actions = self.decide_quotes(snapshot)
            
            # Queue all actions
            for action in actions:
                self._queue_order(action)
            
            # Always send DONE
            self._queue_order({"action": "DONE"})
        
        except Exception as e:
            print(f"[{self.student_id}] Market data error: {e}")
    
    def decide_quotes(self, snapshot: Dict) -> list:
        """
        Two-sided quoting strategy with queue priority logic
        Returns list of actions (cancels + new orders)
        """
        actions = []
        
        bid = snapshot['bid']
        ask = snapshot['ask']
        mid = snapshot['mid']
        inventory = snapshot['inventory']
        step = snapshot['step']
        
        if bid <= 0 or ask <= 0:
            return actions
        
        # Calculate signals
        microprice = SignalCalculator.calculate_microprice(
            bid, ask, snapshot['bid_size'], snapshot['ask_size']
        )
        imbalance = SignalCalculator.calculate_imbalance(
            snapshot['bid_size'], snapshot['ask_size']
        )
        adverse_score = SignalCalculator.detect_adverse_selection(
            self.state.fill_history, mid
        )
        
        # Emergency flatten
        if abs(inventory) >= self.EMERGENCY_INVENTORY:
            # Cancel everything and cross spread
            if snapshot['quotes']['bid']:
                actions.append({"action": "CANCEL", "order_id": snapshot['quotes']['bid'].order_id})
            if snapshot['quotes']['ask']:
                actions.append({"action": "CANCEL", "order_id": snapshot['quotes']['ask'].order_id})
            
            if inventory > 0:
                # Panic sell
                actions.append({
                    "order_id": f"EMG_SELL_{step}",
                    "side": "SELL",
                    "price": round(bid - 0.10, 2),
                    "qty": min(500, inventory)
                })
            else:
                # Panic buy
                actions.append({
                    "order_id": f"EMG_BUY_{step}",
                    "side": "BUY",
                    "price": round(ask + 0.10, 2),
                    "qty": min(500, -inventory)
                })
            return actions
        
        # Calculate fair value
        fair_value = microprice if microprice > 0 else mid
        
        # Inventory skew
        inv_ratio = inventory / self.MAX_INVENTORY
        skew = inv_ratio * 0.10
        fair_value -= skew
        
        # Edge calculation
        spread = ask - bid
        base_edge = max(self.MIN_EDGE, spread * 0.35)
        
        # Widen edge if adverse selection detected
        if adverse_score > self.ADVERSE_SELECTION_THRESHOLD:
            base_edge *= (1 + adverse_score * 20)
        
        # Adjust for imbalance
        edge_bid = base_edge * (1 - imbalance * 0.5)
        edge_ask = base_edge * (1 + imbalance * 0.5)
        
        # Target prices
        target_bid = round(fair_value - edge_bid, 2)
        target_ask = round(fair_value + edge_ask, 2)
        
        # Inventory limits
        can_buy = inventory + 100 <= self.MAX_INVENTORY
        can_sell = inventory - 100 >= -self.MAX_INVENTORY
        
        # Queue priority logic for BID
        current_bid = snapshot['quotes']['bid']
        should_replace_bid = True
        
        if current_bid:
            # Check if at top of book
            at_top_of_book = abs(current_bid.price - bid) < 0.01
            stable_steps = snapshot['bid_stable_steps']
            
            if at_top_of_book and stable_steps >= self.QUEUE_AGE_THRESHOLD:
                # Keep order - we have queue priority
                if abs(current_bid.price - target_bid) < 0.05:
                    should_replace_bid = False
            
            # Always cancel if price moved away significantly
            if bid > 0 and abs(current_bid.price - bid) > spread * 0.5:
                actions.append({"action": "CANCEL", "order_id": current_bid.order_id})
                should_replace_bid = True
        
        # Queue priority logic for ASK
        current_ask = snapshot['quotes']['ask']
        should_replace_ask = True
        
        if current_ask:
            at_top_of_book = abs(current_ask.price - ask) < 0.01
            stable_steps = snapshot['ask_stable_steps']
            
            if at_top_of_book and stable_steps >= self.QUEUE_AGE_THRESHOLD:
                if abs(current_ask.price - target_ask) < 0.05:
                    should_replace_ask = False
            
            if ask > 0 and abs(current_ask.price - ask) > spread * 0.5:
                actions.append({"action": "CANCEL", "order_id": current_ask.order_id})
                should_replace_ask = True
        
        # Send new quotes if needed
        if should_replace_bid and can_buy:
            if current_bid and current_bid.order_id not in [a.get("order_id") for a in actions if a.get("action") == "CANCEL"]:
                actions.append({"action": "CANCEL", "order_id": current_bid.order_id})
            
            bid_order_id = f"BID_{step}_{int(time.time()*1000)}"
            actions.append({
                "order_id": bid_order_id,
                "side": "BUY",
                "price": target_bid,
                "qty": 100
            })
            
            # Update state
            order = Order(bid_order_id, OrderSide.BUY, target_bid, 100, step, time.time())
            self.state.set_quote(OrderSide.BUY, order)
        
        if should_replace_ask and can_sell:
            if current_ask and current_ask.order_id not in [a.get("order_id") for a in actions if a.get("action") == "CANCEL"]:
                actions.append({"action": "CANCEL", "order_id": current_ask.order_id})
            
            ask_order_id = f"ASK_{step}_{int(time.time()*1000)}"
            actions.append({
                "order_id": ask_order_id,
                "side": "SELL",
                "price": target_ask,
                "qty": 100
            })
            
            # Update state
            order = Order(ask_order_id, OrderSide.SELL, target_ask, 100, step, time.time())
            self.state.set_quote(OrderSide.SELL, order)
        
        return actions
    
    def _on_order_response(self, ws, message: str):
        """Handle order responses - ONLY updates state"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "AUTHENTICATED":
                print(f"[{self.student_id}] Authenticated - ready to trade!")
            
            elif msg_type == "FILL":
                qty = data.get("qty", 0)
                price = data.get("price", 0.0)
                side_str = data.get("side", "")
                order_id = data.get("order_id", "")
                
                side = OrderSide.BUY if side_str == "BUY" else OrderSide.SELL
                
                # Thread-safe inventory update
                self.state.update_inventory(side, qty, price)
                
                # Clear quote
                if "BID_" in order_id:
                    self.state.clear_quote(OrderSide.BUY)
                elif "ASK_" in order_id:
                    self.state.clear_quote(OrderSide.SELL)
                
                snapshot = self.state.get_snapshot()
                print(
                    f"[{self.student_id}] FILL: {side_str} {qty} @ {price:.2f} | "
                    f"Inv: {snapshot['inventory']} | PnL: {snapshot['pnl']:.2f}"
                )
            
            elif msg_type in ("REJECTED", "CANCELLED"):
                order_id = data.get("order_id", "")
                
                # Clear quote if it was ours
                if "BID_" in order_id:
                    self.state.clear_quote(OrderSide.BUY)
                elif "ASK_" in order_id:
                    self.state.clear_quote(OrderSide.SELL)
            
            elif msg_type == "ERROR":
                print(f"[{self.student_id}] ERROR: {data.get('message')}")
        
        except Exception as e:
            print(f"[{self.student_id}] Order response error: {e}")
    
    def _on_error(self, ws, error):
        if self.running:
            print(f"[{self.student_id}] WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        self.running = False
        print(f"[{self.student_id}] Connection closed (status: {close_status_code})")
    
    def run(self):
        """Main entry point"""
        if not self.register():
            return
        if not self.connect():
            return
        
        print(f"[{self.student_id}] Running... Press Ctrl+C to stop")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.student_id}] Stopped by user")
        finally:
            self.running = False
            if self.market_ws:
                self.market_ws.close()
            if self.order_ws:
                self.order_ws.close()
            
            snapshot = self.state.get_snapshot()
            print(f"\n[{self.student_id}] Final Results:")
            print(f"  Orders Sent: {snapshot['orders_sent']}")
            print(f"  Fills: {snapshot['fills']}")
            print(f"  Inventory: {snapshot['inventory']}")
            print(f"  PnL: {snapshot['pnl']:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Advanced Market-Making Algorithm")
    parser.add_argument("--name", required=True, help="Your team name")
    parser.add_argument("--password", required=True, help="Your team password")
    parser.add_argument("--scenario", default="normal_market", help="Scenario to run")
    parser.add_argument("--host", default="localhost:8080", help="Server host:port")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS/WSS")
    args = parser.parse_args()
    
    bot = TradingBot(
        student_id=args.name,
        host=args.host,
        scenario=args.scenario,
        password=args.password,
        secure=args.secure
    )
    
    bot.run()