"""统一配置 HTTP 代理，供全部 urllib REST 请求共用。"""

from __future__ import annotations

from urllib.parse import urlparse
from urllib.request import ProxyHandler, build_opener, install_opener


def configure_network_proxy(enabled: bool, proxy_url: str) -> None:
    """安装启用或禁用代理后的全局 urllib opener。"""
    if enabled:
        parsed = urlparse(proxy_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("代理地址必须是有效的 http 或 https URL。")
        install_opener(build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url})))
        return
    install_opener(build_opener(ProxyHandler({})))
