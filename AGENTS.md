# Binance API Source of Truth

For every Binance API integration in this repository, use Binance's official
developer documentation as the authoritative source:

- Documentation index: https://developers.binance.com/en/docs/llms.txt
- Complete machine-readable documentation: https://developers.binance.com/en/docs/llms-full.txt

Before adding or changing an endpoint, WebSocket stream, request signing flow,
rate-limit handling, parameter, response schema, or environment setting,
consult the applicable official Binance documentation. Do not rely on
third-party documentation, unofficial endpoints, or remembered API details
when they conflict with or are absent from the official source.

Record the specific official documentation URL alongside any non-obvious API
integration decision when doing so improves maintainability.

# Code Comment Language

Write code comments primarily in Chinese. Keep established technical terms,
protocol names, API field names, library identifiers, and other terminology in
English when translating them would reduce clarity or accuracy.

# Git Tracking Policy

All program source files and project configuration files must be tracked by
Git. Do not add generated runtime output to Git, including caches, compiled
artifacts, logs, reports, temporary files, local virtual environments, and
runtime state. Keep `.gitignore` limited to those generated or machine-local
artifacts so that source and configuration are not accidentally excluded.
