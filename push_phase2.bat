@echo off
echo ============================================
echo  Pushing Purchase Plan (Phase 2) to GitHub
echo ============================================
cd /d "%~dp0"

echo.
echo Adding Purchase Plan files...
git add Controllers/PurchasePlanController.cs
git add Controllers/DelPendingController.cs
git add Models/PurchasePlan.cs
git add Models/DelPending.cs
git add Models/PurchasePlanExecutionParams.cs
git add Views/PurchasePlan/Execute.cshtml
git add Views/PurchasePlan/Output.cshtml
git add Views/DelPending/Index.cshtml
git add Views/DelPending/Create.cshtml
git add Views/DelPending/Edit.cshtml
git add Views/DelPending/Delete.cshtml
git add Data/PlanningDbContext.cs
git add Services/PlanService.cs
git add Views/Shared/_Layout.cshtml

echo.
echo Committing...
git commit -m "Add Purchase Plan portal (Phase 2) - RDC x MAJ_CAT x WEEK"

echo.
echo Pushing to GitHub...
git push origin main

echo.
echo ============================================
echo  Done! Check https://github.com/V2-Application/TRANSFER_IN_PLAN
echo ============================================
pause
