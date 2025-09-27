Python is available in .venv/bin/python

Use altral uv to run scripts: uv run script.py

Use maturin develop for building the Python Rust extension.

Identify if you are stuck. If you get stuck, add more debug print statements.

The rust pyo3 code will not work when using println. You must write to stdout with a debug_print! macro that is defined using print statements and finally flushes stdout.

Always set timeouts for tests. Tests can unexpectedly hang and hanging is UNACCEPTABLE.

Tests should always complete within 60 seconds, preferably 10.

Do not remove debug print statements.


**Tools and Commands:**
- Build: `maturin develop`
- Spec management: `openspec list`, `openspec validate`, `openspec show`

Use uv extensively when dealing with Python code. Use `uv add` to add packages to the local virtual environment and launch python scripts with `uv run`

NEVER modify sys.path without my explicit permission.
