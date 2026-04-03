@echo off
echo ============================================
echo  Pushing TRANSFER_IN_PLAN to GitHub...
echo ============================================
cd /d "%~dp0"
git init
git add -A
git commit -m "ASP.NET Core 8.0 MVC Transfer In Plan Portal"
git branch -M main
git remote add origin https://github.com/V2-Application/TRANSFER_IN_PLAN.git
git push -u origin main --force
echo.
echo ============================================
echo  Done! Check https://github.com/V2-Application/TRANSFER_IN_PLAN
echo ============================================
pause
