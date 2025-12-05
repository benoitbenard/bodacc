@echo off
chcp 65001 > nul
setlocal ENABLEDELAYEDEXPANSION

REM === Python executable (adapter si nécessaire) ===
set PYTHON="C:\dataToolBox\Python\python.exe"

REM === Répertoire du script ===
set SCRIPT_DIR=%~dp0
pushd "%SCRIPT_DIR%"

REM === Paramètre optionnel : chemin vers config.ini ===
set ARG_CONFIG=%~1
if NOT "%ARG_CONFIG%"=="" (
    echo Parametre config detecte : %ARG_CONFIG%
)

echo.
echo ===========================================================
echo ========   Lancement du pipeline BODACC (01→03)   ========
echo ===========================================================

echo.
set MODULES=bodacc.main.01_get_SIREN_from_SEMARCHY_MDM bodacc.main.02_get_BODACC_by_day bodacc.main.03_filter_BODACC_by_day

for %%M in (%MODULES%) do (
    echo -----------------------------------------------------------
    echo === Lancement : %%M
    echo -----------------------------------------------------------

    REM ------------------------------------------------------------
    REM Construction de la ligne de commande Python
    REM ------------------------------------------------------------

    set CMD=%PYTHON% -m %%M

    if NOT "%ARG_CONFIG%"=="" (
        set CMD=!CMD! --config "%ARG_CONFIG%"
    )

    if NOT "%ARG_KEY%"=="" (
        set CMD=!CMD! --key "%ARG_KEY%"
    )

    echo Exécution : !CMD!
    echo.

    REM Lancement
    !CMD!
    if ERRORLEVEL 1 (
        echo.
        echo *** ERREUR dans %%M — arrêt du pipeline ***
        echo.
        exit /b 1
    )
    echo.
)

echo ===========================================================
echo =========   Pipeline AfterData terminé avec succès   ========
echo ===========================================================
echo.

endlocal
exit /b %ERRORLEVEL%
