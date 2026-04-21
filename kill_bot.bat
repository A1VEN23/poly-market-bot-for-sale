@echo off
echo Stopping all Python bot instances...
taskkill /F /IM python.exe /T 2>nul
taskkill /F /IM python3.exe /T 2>nul
echo Done! Wait 5 seconds before starting bot again.
timeout /t 5
