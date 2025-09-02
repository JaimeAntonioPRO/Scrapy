# sitecustomize.py — se carga automáticamente al iniciar Python si está en sys.path
import sys, asyncio
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        # Si ya hay loop/policy, lo ignoramos
        pass