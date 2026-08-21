"""从 Binance 成交明细回算已落盘程序仓位的历史盈亏。

仅处理已有 pnl/*.json 中已登记的 recordId；按 entryOrderId 与 exitOrderIds 精确查询
开平两侧 userTrades，手续费统一记为负支出。写回前会创建完整备份，且任何一条记录
查询失败时不写入任何 PnL 文件。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from binance_futures_client import BinanceFuturesClient
from network_utils import configure_network_proxy
from secret_utils import get_secret


API_KEY_NAME = "BINANCE_API_KEY"
API_SECRET_NAME = "BINANCE_API_SECRET"
LOOKBACK_MS = 60_000


def _number(value: Any, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"字段 {field} 无效:{value!r}") from exc


def _integer(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"字段 {field} 无效:{value!r}") from exc


def _order_trades(
    client: BinanceFuturesClient, symbol: str, order_id: int, start_time: int, end_time: int,
) -> list[dict[str, Any]]:
    """精确读取一个订单的全部成交；空响应视为失败，不能据此篡改审计账本。"""
    trades = client.get_user_trades(symbol, start_time, end_time, order_id=order_id)
    if not trades:
        raise RuntimeError(f"交易所未返回成交明细 symbol={symbol} orderId={order_id}")
    return trades


def _income_total(
    client: BinanceFuturesClient, symbol: str, start_time: int, end_time: int, income_type: str,
) -> float:
    """汇总一种交易所收入流水，保留交易所的原始正负符号。"""
    total = 0.0
    for page in range(1, 101):
        items = client.get_income(symbol, start_time, end_time, income_type, page=page)
        total += sum(_number(item.get("income"), f"{income_type} income") for item in items)
        if len(items) < 1000:
            return total
    raise RuntimeError(f"收入流水分页超过安全上限 symbol={symbol} type={income_type}")


def _commission_income(
    client: BinanceFuturesClient, symbol: str, start_time: int, end_time: int,
) -> tuple[float, dict[str, float]]:
    """从交易所 COMMISSION 流水汇总手续费，并按资产保留原始符号。"""
    by_asset: dict[str, float] = {}
    for page in range(1, 101):
        items = client.get_income(symbol, start_time, end_time, "COMMISSION", page=page)
        for item in items:
            asset = str(item.get("asset") or "USDT")
            income = _number(item.get("income"), "COMMISSION income")
            by_asset[asset] = by_asset.get(asset, 0.0) + income
        if len(items) < 1000:
            return by_asset.get("USDT", 0.0), by_asset
    raise RuntimeError(f"手续费流水分页超过安全上限 symbol={symbol}")


def _recalculate_record(client: BinanceFuturesClient, record: dict[str, Any]) -> dict[str, Any]:
    """以订单 ID 精确回算一条程序仓位记录，不扫描任何人工仓位。"""
    symbol = str(record.get("symbol") or "")
    if not symbol:
        raise ValueError("PnL 记录缺少 symbol")
    opened_at = _integer(record.get("openTime"), "openTime")
    closed_at = _integer(record.get("closeTime"), "closeTime")
    entry_order_id = _integer(record.get("entryOrderId"), "entryOrderId")
    exit_order_ids = record.get("exitOrderIds")
    if not isinstance(exit_order_ids, list) or not exit_order_ids:
        raise ValueError(f"PnL 记录缺少 exitOrderIds symbol={symbol}")
    start_time = max(0, opened_at - LOOKBACK_MS)
    end_time = closed_at + LOOKBACK_MS
    all_trades: list[dict[str, Any]] = _order_trades(client, symbol, entry_order_id, start_time, end_time)
    for value in exit_order_ids:
        all_trades.extend(_order_trades(client, symbol, _integer(value, "exitOrderId"), start_time, end_time))

    deduplicated: dict[str, dict[str, Any]] = {}
    for trade in all_trades:
        trade_id = str(trade.get("id") or trade.get("tradeId") or "")
        if not trade_id:
            raise ValueError(f"成交明细缺少 tradeId symbol={symbol}")
        deduplicated[trade_id] = trade
    realized = sum(_number(item.get("realizedPnl", 0), "realizedPnl") for item in deduplicated.values())
    by_asset: dict[str, float] = {}
    for item in deduplicated.values():
        asset = str(item.get("commissionAsset") or "USDT")
        # Binance userTrades 的 commission 是金额，不以其字符串正负决定费用方向。
        expense = -abs(_number(item.get("commission", 0), "commission"))
        by_asset[asset] = by_asset.get(asset, 0.0) + expense
    non_usdt_assets = sorted(asset for asset in by_asset if asset != "USDT")
    if non_usdt_assets:
        raise ValueError(f"存在非 USDT 手续费，无法无汇率换算: {symbol} {non_usdt_assets}")
    trade_commission = by_asset.get("USDT", 0.0)
    commission, income_by_asset = _commission_income(client, symbol, start_time, end_time)
    income_non_usdt_assets = sorted(asset for asset in income_by_asset if asset != "USDT")
    if income_non_usdt_assets:
        raise ValueError(f"手续费收入流水存在非 USDT 资产，无法无汇率换算: {symbol} {income_non_usdt_assets}")
    # userTrades 按订单 ID 精确归属，COMMISSION 是账户资金流水；两者不一致说明
    # 时间范围、成交归属或交易所数据仍存在缺口，拒绝写入而非用猜测覆盖审计账本。
    if abs(trade_commission - commission) > 1e-8:
        raise RuntimeError(
            f"手续费成交/流水不一致 symbol={symbol} trades={trade_commission} income={commission}"
        )
    funding = _income_total(client, symbol, opened_at, closed_at, "FUNDING_FEE")
    result = dict(record)
    result.update({
        "realizedPnlUsdt": realized,
        "commissionUsdt": commission,
        "fundingFeeUsdt": funding,
        "commissionByAsset": income_by_asset,
        "netPnlUsdt": realized + commission + funding,
        "historicalRecalculatedAt": datetime.now(timezone.utc).isoformat(),
        "historicalRecalculation": "entry/exit userTrades 与 COMMISSION/FUNDING_FEE 收入流水交叉校验",
    })
    return result


def _audit_record(client: BinanceFuturesClient, record: dict[str, Any]) -> dict[str, Any]:
    """只读比对成交明细手续费与 Commission/Funding 收入流水，不修改文件。"""
    recalculated = _recalculate_record(client, record)
    symbol = str(record["symbol"])
    opened_at = _integer(record["openTime"], "openTime")
    closed_at = _integer(record["closeTime"], "closeTime")
    income_commission, _ = _commission_income(client, symbol, max(0, opened_at - LOOKBACK_MS), closed_at + LOOKBACK_MS)
    return {
        "recordId": record.get("recordId"),
        "symbol": symbol,
        "tradeCommission": recalculated["commissionUsdt"],
        "incomeCommission": income_commission,
        "commissionDelta": recalculated["commissionUsdt"] - income_commission,
        "tradeFunding": recalculated["fundingFeeUsdt"],
        "storedNet": record.get("netPnlUsdt"),
        "recalculatedNet": recalculated["netPnlUsdt"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="回算 production 历史 PnL 的开平手续费与净盈亏。")
    parser.add_argument("--pnl-directory", type=Path, required=True)
    parser.add_argument("--proxy-url", default="http://127.0.0.1:7892")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--audit", action="store_true", help="仅比对交易成交与收入流水，不创建备份或写文件。")
    parser.add_argument("--record-id", action="append", default=[], help="仅处理指定 recordId（可重复指定）。")
    arguments = parser.parse_args()
    pnl_directory = arguments.pnl_directory.resolve()
    files = sorted(pnl_directory.glob("*.json"))
    if not files:
        raise RuntimeError(f"未找到 PnL 文件: {pnl_directory}")
    api_key = get_secret(API_KEY_NAME)
    api_secret = get_secret(API_SECRET_NAME)
    if not api_key or not api_secret:
        raise RuntimeError("缺少生产环境 Binance API 密钥。")
    configure_network_proxy(bool(arguments.proxy_url), arguments.proxy_url)
    client = BinanceFuturesClient(api_key, api_secret, use_testnet=False, timeout=arguments.timeout)
    selected_ids = set(arguments.record_id)

    if arguments.audit:
        report: list[dict[str, Any]] = []
        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            records = payload.get("records") if isinstance(payload, dict) else None
            if not isinstance(records, list):
                raise ValueError(f"PnL 文件 records 格式无效: {path}")
            report.extend(
                _audit_record(client, record)
                for record in records
                if isinstance(record, dict) and (not selected_ids or str(record.get("recordId")) in selected_ids)
            )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    # 所有 REST 查询先完成；有任何异常即退出，绝不留下只改了一半的本地账本。
    replacement: dict[Path, dict[str, Any]] = {}
    changed = 0
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            raise ValueError(f"PnL 文件 records 格式无效: {path}")
        result_records = []
        for record in records:
            if not isinstance(record, dict):
                raise ValueError(f"PnL 记录格式无效: {path}")
            if selected_ids and str(record.get("recordId")) not in selected_ids:
                result_records.append(record)
            else:
                result_records.append(_recalculate_record(client, record))
                changed += 1
        replacement[path] = {**payload, "records": result_records}

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_directory = pnl_directory.parent / f"pnl-backup-full-recalc-{stamp}"
    if backup_directory.exists():
        raise RuntimeError(f"备份目录已存在: {backup_directory}")
    backup_directory.mkdir(parents=True)
    for path in files:
        (backup_directory / path.name).write_bytes(path.read_bytes())

    for path, payload in replacement.items():
        # 写入前重新读取，保留运行中新增的记录；仅替换本次明确回算过的 recordId。
        current = json.loads(path.read_text(encoding="utf-8"))
        current_records = current.get("records") if isinstance(current, dict) else None
        if not isinstance(current_records, list):
            raise ValueError(f"写入前 PnL 文件 records 格式无效: {path}")
        refreshed = {str(item.get("recordId")): item for item in payload["records"] if isinstance(item, dict)}
        current["records"] = [
            refreshed.get(str(item.get("recordId")), item) if isinstance(item, dict) else item
            for item in current_records
        ]
        temporary = path.with_suffix(path.suffix + ".full-recalc.tmp")
        temporary.write_text(json.dumps(current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    print(json.dumps({"backup": str(backup_directory), "updatedRecords": changed}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"历史 PnL 回算失败: {exc}", file=sys.stderr)
        raise SystemExit(1)
