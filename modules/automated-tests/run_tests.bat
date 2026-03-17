@echo off
REM Get current date in YYYY-MM-DD format
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do set mydate=%%c-%%a-%%b
REM Remove possible quotes and spaces
set mydate=%mydate: =%
set mydate=%mydate:"=%

REM Launch tests and save results with date in filename
echo 'Launching automated tests... This may take a while.'
if not exist "test-results" mkdir test-results
pytest --self-contained-html --html=test-results\test-results-%mydate%.html
