@echo off
setlocal

REM Gå till projektmappen
cd /d C:\Users\CD\Trafikverket\pipeline\

REM Kör i conda-miljön utan att aktivera den
C:\Users\CD\anaconda3\Scripts\conda.exe run -n base python run_production_pipeline.py

exit /b %ERRORLEVEL%
