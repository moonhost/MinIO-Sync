@echo off
call "%~dp0.venv\Scripts\activate.bat"
"%~dp0.venv\Scripts\minio-sync.exe"
pause