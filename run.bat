@echo off
cd /d "%~dp0"
uvicorn static.server:app --reload --host 0.0.0.0 --port 8000
pause
