"""
Student Trading Algorithm Template - FIXED
===========================================
Connect to the exchange simulator, receive market data, and submit orders.

FIXES:
- Reduced ORDER_TTL_STEPS to 10 (more aggressive cleanup)
- Added better inventory limits
- Improved self-trade prevention
- Better error handling for open orders
"""

import json
import websocket
import threading
import argparse
import time
import requests
import ssl
import urllib3
from typing import Dict, Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TradingBot:
    """
    A trading bot that connects to the exchange simulator.
    Students should modify the `decide_order()` method to implement their strategy.
    """

    def __init__(self, student_id: str, host: str, scenario: str, password: str = None, secure: bool = False):
        self.student_id = student_id
        self.host = host
        self.scenario = scenario
        self.password = password
        self.secure = secure

        # Protocol configuration
        self.http_proto = "https" if secure else "http"
        self.ws_proto = "wss" if secure else "ws"

        # Session info
        self.token = None
        self.run_id = None

        # Trading state
        self.inventory = 0
        self.cash_flow = 0.0
        self.pnl = 0.0
        self.current_step = 0
        self.orders_sent = 0
        self.total_fills = 0

        # ============================================================
        # ORDER MANAGEMENT (FIXED)
        # ============================================================
        self.open_orders = {}              # order_id -> {side, price, qty, step}
        self.MAX_OPEN_ORDERS = 45          # Stay below exchange limit of 50
        self.ORDER_TTL_STEPS = 10          # REDUCED from 25 - more aggressive cleanup
        self.MAX_INVENTORY = 300           # REDUCED - prevent hitting server limits

        # Market data
        self.last_bid = 0.0
        self.last_ask = 0.0
        self.last_mid = 0.0
        self.price_history = []

        # WebSocket connections
        self.market_ws = None
        self.order_ws = None
        self.running = True

        # Latency measurement
        self.last_done_time = None
        self.step_latencies = []
        self.order_send_times = {}
        self.fill_latencies = []

    def register(self) -> bool:
        """Register with the server and get an auth token."""
        print(f"[{self.student_id}] Registering for scenario '{self.scenario}'...")
        try:
            url = f"{self.http_proto}://{self.host}/api/replays/{self.scenario}/start"
            headers = {"Authorization": f"Bearer {self.student_id}"}
            if self.password:
                headers["X-Team-Password"] = self.password

            resp = requests.get(
                url,
                headers=headers,
                timeout=10,
                verify=not self.secure
            )

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
        """Connect to market data and order entry WebSockets."""
        try:
            sslopt = {"cert_reqs": ssl.CERT_NONE} if self.secure else None

            # Market data WS
            market_url = f"{self.ws_proto}://{self.host}/api/ws/market?run_id={self.run_id}"
            self.market_ws = websocket.WebSocketApp(
                market_url,
                on_message=self._on_market_data,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Market data connected")
            )

            # Orders WS
            order_url = f"{self.ws_proto}://{self.host}/api/ws/orders?token={self.token}&run_id={self.run_id}"
            self.order_ws = websocket.WebSocketApp(
                order_url,
                on_message=self._on_order_response,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Order entry connected")
            )

            threading.Thread(
                target=lambda: self.market_ws.run_forever(sslopt=sslopt),
                daemon=True
            ).start()

            threading.Thread(
                target=lambda: self.order_ws.run_forever(sslopt=sslopt),
                daemon=True
            ).start()

            time.sleep(1)
            return True

        except Exception as e:
            print(f"[{self.student_id}] Connection error: {e}")
            return False

    def _cleanup_open_orders(self):
        """
        IMPROVED: More aggressive cleanup of stale orders.
        Remove orders that are older than ORDER_TTL_STEPS.
        """
        if not self.open_orders:
            return

        cutoff = self.current_step - self.ORDER_TTL_STEPS
        stale = [oid for oid, o in self.open_orders.items() if o.get("step", 0) <= cutoff]
        
        if stale:
            for oid in stale:
                del self.open_orders[oid]
            if len(stale) > 5:  # Only log if significant cleanup
                print(f"[{self.student_id}] Cleaned up {len(stale)} stale orders")

    def _on_market_data(self, ws, message: str):
        """Handle incoming market data snapshot."""
        try:
            recv_time = time.time()
            data = json.loads(message)

            if data.get("type") == "CONNECTED":
                return

            # latency
            if self.last_done_time is not None:
                step_latency = (recv_time - self.last_done_time) * 1000
                self.step_latencies.append(step_latency)

            # market
            self.current_step = data.get("step", 0)
            self.last_bid = data.get("bid", 0.0)
            self.last_ask = data.get("ask", 0.0)

            # cleanup stale orders BEFORE deciding new orders
            self._cleanup_open_orders()

            if self.current_step % 500 == 0:
                avg_lat = sum(self.step_latencies[-100:]) / min(len(self.step_latencies), 100) if self.step_latencies else 0
                print(
                    f"[{self.student_id}] Step {self.current_step} | Orders: {self.orders_sent} | "
                    f"Fills: {self.total_fills} | Open: {len(self.open_orders)} | "
                    f"Inv: {self.inventory} | PnL: {self.pnl:.2f} | Latency: {avg_lat:.1f}ms"
                )

            # mid
            if self.last_bid > 0 and self.last_ask > 0:
                self.last_mid = (self.last_bid + self.last_ask) / 2
            elif self.last_bid > 0:
                self.last_mid = self.last_bid
            elif self.last_ask > 0:
                self.last_mid = self.last_ask
            else:
                self.last_mid = 0

            # strategy
            order = self.decide_order(self.last_bid, self.last_ask, self.last_mid)

            if order and self.order_ws and self.order_ws.sock:
                self._send_order(order)

            self._send_done()

        except Exception as e:
            print(f"[{self.student_id}] Market data error: {e}")

    def decide_order(self, bid: float, ask: float, mid: float) -> Optional[Dict]:

        if len(self.open_orders) >= self.MAX_OPEN_ORDERS:
            return None

        if abs(self.inventory) >= self.MAX_INVENTORY:
            return None

        if mid > 0:
            self.price_history.append(mid)
        if len(self.price_history) > 20:
            self.price_history.pop(0)

        if len(self.price_history) < 5:
            return None

        # 2) Calculate fair value
        fair_value = sum(self.price_history) / len(self.price_history)

        # 3) Parameters - REDUCED order size
        order_qty = 100  # REDUCED from 100

        # 4) Inventory skew
        inventory_skew = self.inventory / self.MAX_INVENTORY
        target_price = fair_value - (inventory_skew * 0.05)

        # 5) Determine side and price
        # Emergency unwind if inventory is high
        if self.inventory > (self.MAX_INVENTORY * 0.7):
            side = "SELL"
            price = round(bid - 0.01, 2)  # Aggressive sell
        elif self.inventory < -(self.MAX_INVENTORY * 0.7):
            side = "BUY"
            price = round(ask + 0.01, 2)  # Aggressive buy
        else:
            # Normal market making
            if mid > target_price:
                side = "SELL"
                price = round(max(bid, mid - 0.01), 2)
            else:
                side = "BUY"
                price = round(min(ask, mid + 0.01), 2)

        # ==========================================================
        # SELF-TRADE PREVENTION
        # ==========================================================
        best_own_buy = None
        best_own_sell = None

        for _, o in self.open_orders.items():
            if o["side"] == "BUY":
                if best_own_buy is None or o["price"] > best_own_buy:
                    best_own_buy = o["price"]
            elif o["side"] == "SELL":
                if best_own_sell is None or o["price"] < best_own_sell:
                    best_own_sell = o["price"]

        # Don't cross our own orders
        if side == "BUY" and best_own_sell is not None and price >= best_own_sell:
            return None

        if side == "SELL" and best_own_buy is not None and price <= best_own_buy:
            return None

        return {"side": side, "price": price, "qty": order_qty}

    def _send_order(self, order: Dict):
        """Send an order to the exchange."""
        # Double-check limits
        if len(self.open_orders) >= self.MAX_OPEN_ORDERS:
            return

        if abs(self.inventory) >= self.MAX_INVENTORY:
            return

        order_id = f"ORD_{self.student_id}_{self.current_step}_{self.orders_sent}"

        msg = {
            "order_id": order_id,
            "side": order["side"],
            "price": order["price"],
            "qty": order["qty"]
        }

        try:
            self.order_send_times[order_id] = time.time()

            # Track open order
            self.open_orders[order_id] = {
                "side": order["side"],
                "price": order["price"],
                "qty": order["qty"],
                "step": self.current_step
            }

            self.order_ws.send(json.dumps(msg))
            self.orders_sent += 1

        except Exception as e:
            print(f"[{self.student_id}] Send order error: {e}")
            if order_id in self.open_orders:
                del self.open_orders[order_id]

    def _send_done(self):
        """Signal DONE to advance to next simulation step."""
        try:
            self.order_ws.send(json.dumps({"action": "DONE"}))
            self.last_done_time = time.time()
        except:
            pass

    def _on_order_response(self, ws, message: str):
        """Handle order responses and fills."""
        try:
            recv_time = time.time()
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "AUTHENTICATED":
                print(f"[{self.student_id}] Authenticated - ready to trade!")

            elif msg_type == "FILL":
                qty = data.get("qty", 0)
                price = data.get("price", 0)
                side = data.get("side", "")
                order_id = data.get("order_id", "")

                # Remove from open orders
                if order_id in self.open_orders:
                    del self.open_orders[order_id]

                # Track fill latency
                if order_id in self.order_send_times:
                    fill_latency = (recv_time - self.order_send_times[order_id]) * 1000
                    self.fill_latencies.append(fill_latency)
                    del self.order_send_times[order_id]

                # Update inventory and cash flow
                if side == "BUY":
                    self.inventory += qty
                    self.cash_flow -= qty * price
                else:
                    self.inventory -= qty
                    self.cash_flow += qty * price

                self.total_fills += 1
                self.pnl = self.cash_flow + self.inventory * self.last_mid

                if self.total_fills % 10 == 0:  # Less verbose
                    print(
                        f"[{self.student_id}] FILL #{self.total_fills}: {side} {qty} @ {price:.2f} | "
                        f"Inv: {self.inventory} | PnL: {self.pnl:.2f}"
                    )

            elif msg_type in ("REJECTED", "CANCELLED"):
                order_id = data.get("order_id", "")
                if order_id in self.open_orders:
                    del self.open_orders[order_id]
                reason = data.get('message', 'Unknown')
                if 'limit' in reason.lower() or 'exceed' in reason.lower():
                    print(f"[{self.student_id}] ⚠️  {msg_type}: {reason}")

            elif msg_type == "ERROR":
                order_id = data.get("order_id", "")
                if order_id in self.open_orders:
                    del self.open_orders[order_id]
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
        """Main entry point - register, connect, and run."""
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

            print(f"\n[{self.student_id}] ===== FINAL RESULTS =====")
            print(f"  Orders Sent: {self.orders_sent}")
            print(f"  Total Fills: {self.total_fills}")
            print(f"  Open Orders: {len(self.open_orders)}")
            print(f"  Final Inventory: {self.inventory}")
            print(f"  Final PnL: {self.pnl:.2f}")

            if self.step_latencies:
                print(f"\n  Step Latency (ms):")
                print(f"    Min: {min(self.step_latencies):.1f}")
                print(f"    Max: {max(self.step_latencies):.1f}")
                print(f"    Avg: {sum(self.step_latencies)/len(self.step_latencies):.1f}")

            if self.fill_latencies:
                print(f"\n  Fill Latency (ms):")
                print(f"    Min: {min(self.fill_latencies):.1f}")
                print(f"    Max: {max(self.fill_latencies):.1f}")
                print(f"    Avg: {sum(self.fill_latencies)/len(self.fill_latencies):.1f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Student Trading Algorithm - FIXED VERSION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Local server:
    python student_algorithm.py --name team_alpha --password secret123 --scenario normal_market

  Deployed server (HTTPS):
    python student_algorithm.py --name team_alpha --password secret123 --scenario normal_market --host 3.98.52.120:8433 --secure
        """
    )

    parser.add_argument("--name", required=True, help="Your team name")
    parser.add_argument("--password", required=True, help="Your team password")
    parser.add_argument("--scenario", default="normal_market", help="Scenario to run")
    parser.add_argument("--host", default="localhost:8080", help="Server host:port")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS/WSS (for deployed servers)")
    args = parser.parse_args()

    bot = TradingBot(
        student_id=args.name,
        host=args.host,
        scenario=args.scenario,
        password=args.password,
        secure=args.secure
    )

    bot.run()