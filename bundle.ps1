$ErrorActionPreference = "Stop"

$RepoRoot = (Get-Location).Path
$Out = Join-Path $RepoRoot "bundle_repo.txt"

# Exclusões (ajuste se precisar)
$ExcludeRegex = @(
  '^[\\/]?\.git[\\/]',
  '^[\\/]?\.venv[\\/]',
  '^[\\/]?venv[\\/]',
  '__pycache__',
  '\.pytest_cache',
  '\.mypy_cache',
  '\.ruff_cache',
  '^[\\/]?dist[\\/]',
  '^[\\/]?build[\\/]',
  '^[\\/]?\.idea[\\/]',
  '^[\\/]?\.vscode[\\/]'
) -join '|'

$ExcludeExtRegex = '\.(parquet|csv|zip|gz|png|jpg|jpeg|pdf|exe|dll|pdb)$'

# StreamWriter UTF-8 (sem BOM) — mais robusto que Add-Content
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$sw = New-Object System.IO.StreamWriter($Out, $false, $Utf8NoBom)

function W([string]$s = "") { $sw.WriteLine($s) }

try {
  W "==== REPO BUNDLE ===="
  W ("GeneratedAt: " + (Get-Date).ToString("o"))
  W ("RepoRoot: " + $RepoRoot)
  W

  # Git info
  try {
    $isGit = git rev-parse --is-inside-work-tree 2>$null
    if ($LASTEXITCODE -eq 0) {
      W "---- GIT INFO ----"
      W "Remote:"
      (git remote -v 2>$null) | ForEach-Object { W $_ }
      W
      W "HEAD:"
      W (git rev-parse HEAD 2>$null)
      W
      W "Status:"
      (git status --porcelain=v1 2>$null) | ForEach-Object { W $_ }
      W
    }
  } catch { }

  # Tree (dirs up to 5)
  W "---- TREE (dirs up to 5) ----"
  Get-ChildItem -Recurse -Directory -Depth 5 |
    Select-Object -ExpandProperty FullName |
    Sort-Object |
    ForEach-Object { W $_ }
  W

  # File list
  $files = @()
  try {
    git rev-parse --is-inside-work-tree 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
      $files = git ls-files | Sort-Object
    } else {
      $files = Get-ChildItem -Recurse -File | ForEach-Object { $_.FullName }
    }
  } catch {
    $files = Get-ChildItem -Recurse -File | ForEach-Object { $_.FullName }
  }

  W "---- FILE LIST ----"
  $files | ForEach-Object { W $_ }
  W

  W "---- FILE CONTENTS ----"

  foreach ($f in $files) {
    if ([string]::IsNullOrWhiteSpace($f)) { continue }

    # Normaliza para caminho relativo quando vier do git ls-files
    $rel = $f
    $fullPath = $f

    if (-not (Test-Path $fullPath)) {
      $fullPath = Join-Path $RepoRoot $f
    } else {
      # se veio full path, tenta também derivar um "rel"
      if ($fullPath.StartsWith($RepoRoot)) {
        $rel = $fullPath.Substring($RepoRoot.Length).TrimStart('\','/')
      }
    }

    # Exclude por regex
    if ($rel -match $ExcludeRegex) { continue }
    if ($rel -match $ExcludeExtRegex) { continue }

    if (-not (Test-Path $fullPath)) {
      W
      W ("===== FILE: " + $rel + " =====")
      W "----- SKIPPED (missing on disk) -----"
      continue
    }

    # Tenta ler como texto; se falhar, pula
    $content = $null
    try {
      $content = Get-Content -Raw -LiteralPath $fullPath -ErrorAction Stop
    } catch {
      W
      W ("===== FILE: " + $rel + " =====")
      W "----- SKIPPED (unreadable as text) -----"
      continue
    }

    W
    W ("===== FILE: " + $rel + " =====")
    W "----- BEGIN -----"
    if ($null -eq $content) { $content = "" }

    $sw.Write($content)
    if ($content.Length -gt 0 -and -not $content.EndsWith("`n")) { W }
    W "----- END -----"
  }

} finally {
  $sw.Flush()
  $sw.Close()
}

Write-Host "Bundle gerado em: $Out"