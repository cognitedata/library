@echo off
REM Run from repo root or from this directory. Script dir is used as cwd for pytest.
cd /d "%~dp0"
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do set mydate=%%c-%%a-%%b
set mydate=%mydate: =%
set mydate=%mydate:"=%
echo Launching automated tests... This may take a while.
if not exist "test-results" mkdir test-results
pytest --self-contained-html --html=test-results\test-results-%mydate%.html %*
