"""Manual logo URL overrides for tickers where the primary CDN returns junk.

These URLs are tried FIRST by `data.logo_sources.from_override()` during the
bulk-download phase. The downloader fetches the bytes from these URLs, saves
them to static/logos/, and updates companies.logo_url to /logos/{T}.png — so
no caller needs to apply this dict to the DB anymore.
"""

LOGO_OVERRIDES = {
    "PENG": "https://cdn.prod.website-files.com/6764579f0a24e5a0083f25bb/67bb88245ce879aaca499ddb_schema--penguin-logo.jpg",
    "HWKN": "https://www.hawkinsinc.com/wp-content/uploads/2025/10/Hawkins-logo-300-x-300.jpg",
    "GTM":  "https://img.logo.dev/zoominfo.com?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "AAMI": "https://img.logo.dev/ticker/AAMI?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "FLG":  "https://img.logo.dev/ticker/FLG?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "VSNT": "https://img.logo.dev/ticker/VSNT?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "OPLN": "https://img.logo.dev/ticker/OPLN?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "HTO":  "https://img.logo.dev/ticker/HTO?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "SPY":  "https://img.logo.dev/ticker/SPY?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "NICE": "https://img.logo.dev/ticker/NICE?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
}
