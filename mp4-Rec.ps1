# mp4=$(basename $1)
# for i in recup_dir.1/*.mov
# do
#         cat $1 $i > output/$mp4
#         if ! ffprobe output/$mp4 2>&1 | grep -q NAL
#         then
#                 echo bingo $1 $i
#                 echo mpv output/$mp4
#                 exit
#         fi
# done


# Script for recovering original video files from pairs of fragments: header (mp4) <> mdat (mov)
#     *Fragments can be obtained using the “testdisk” utility for the required device as the first step of the pipeline.

# Attention! For successful recovery of video to the primary physical storage, no new data should be written.
# Please do not use the storage device, or make sure to create a disk image immediately after detecting file loss!

# # Works with mp4/mov(h264).
# Tested for original video files from:
#       - Canon EOS 250D [ body v0.0.0-v0.0.0 / lens v0.0.0-v0.0.0 ]
#       - ...


<#
.SYNOPSIS
  Video restoration (H.264) by byte-wise concatenation of MP4 header + MOV (mdat) fragment.
  Robust validation (ftyp + ffprobe + ffmpeg frames), precise logging, resumable state, and safe disk handling.

.DESCRIPTION
  - Scans for .mp4 (headers) and .mov (mdat fragments) from provided roots, optionally recursive.
  - Ignores temporary and output artifacts during scanning (e.g., *.tmp, *.tmp.mp4, *.part) and hidden/system items.
  - Always validates MP4 headers (fast ftyp check + ffprobe).
  - Candidate pairing order:
      pr=0: exact offset match (f<offset> in both basenames)
      pr=1: near offset (header offset < mov offset, delta <= OffsetNearBytes)
      pr=2: name/digits/prefix + top-N nearest by size
      pr=3: top-N nearest by size only
  - Concatenates header+mdat to a temp file with live progress; validates via ffprobe + decode N frames with ffmpeg.
  - Keeps only valid outputs; logs success/fail in JSONL; maintains recovered_index.json and summary.csv.
  - Skips already recovered headers/MOVs and existing final outputs, both during scan and pairing.

.REQUIREMENTS
  ffmpeg and ffprobe in PATH.

#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [string[]]$Paths = @("."),

  [switch]$Recurse,                   # recursive scan (excludes temp/output/hidden/system)

  [ValidateSet('mixed','name','size')]
  [string]$PairingMode = 'mixed',

  [int]$MaxPairs = 0,                 # 0 = no limit

  [double]$MinFreeGB = -1,            # <=0: auto = (Header+Mov)*1.24

  [string]$OutputDir = ".\output",

  [string]$StateDir = ".\state",

  [int]$FPS = 25,                     # number of frames to decode (validation)

  [long]$OffsetNearBytes = 64MB,      # pr=1 window size

  [int]$SizeTopN = 5,                 # top-N nearest by size per header

  [switch]$DryRun,                    # plan/log only

  [switch]$ForceRecheck,              # ignore caches and re-try

  [switch]$VerboseLogs                # include Meta in logs
)

$ErrorActionPreference = 'Stop'

# ---------- Tools ----------
function Get-ToolPath {
  param([Parameter(Mandatory=$true)][string]$Name)
  $p = (Get-Command $Name -ErrorAction SilentlyContinue)?.Source
  if (-not $p) { throw "Не найден '$Name' в PATH. Установи и добавь в PATH (ffmpeg/ffprobe)." }
  $p
}
$ffprobe = Get-ToolPath -Name 'ffprobe'
$ffmpeg  = Get-ToolPath -Name 'ffmpeg'

# ---------- Dirs / state ----------
$null = New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$null = New-Item -ItemType Directory -Force -Path $StateDir  | Out-Null

$ValidPairsPath     = Join-Path $StateDir 'valid_pairs.jsonl'
$FailedPairsPath    = Join-Path $StateDir 'failed_pairs.jsonl'
$InvalidHeadersPath = Join-Path $StateDir 'invalid_headers.jsonl'
$IndexPath          = Join-Path $StateDir 'recovered_index.json'
$SummaryCsvPath     = Join-Path $StateDir 'summary.csv'

if (-not (Test-Path $ValidPairsPath))     { New-Item -ItemType File -Path $ValidPairsPath     | Out-Null }
if (-not (Test-Path $FailedPairsPath))    { New-Item -ItemType File -Path $FailedPairsPath    | Out-Null }
if (-not (Test-Path $InvalidHeadersPath)) { New-Item -ItemType File -Path $InvalidHeadersPath | Out-Null }
if (-not (Test-Path $IndexPath))          { '{}' | Set-Content -Path $IndexPath -Encoding UTF8 }
if (-not (Test-Path $SummaryCsvPath))     { "timestamp,output,mp4_header,mov_fragment,size_bytes,duration,video_codec,audio_codec,avg_fps,concat_ms,validate_ms,bytes_per_sec" | Set-Content -Path $SummaryCsvPath -Encoding UTF8 }

# ---------- Logger ----------
$Script:LogLock = New-Object System.Object
function Write-Log {
  param(
    [Parameter(Mandatory=$true)][ValidateSet('INFO','OK','WARN','ERROR','SKIP','STAT')]
    [string]$Level,
    [Parameter(Mandatory=$true)][string]$Message,
    [hashtable]$Meta
  )
  $color = switch ($Level) {
    'INFO'  { 'White' }
    'OK'    { 'Green' }
    'WARN'  { 'Yellow' }
    'ERROR' { 'Red' }
    'SKIP'  { 'Cyan' }
    'STAT'  { 'Magenta' }
  }
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
  $line = "[$ts][$Level] $Message"
  if ($VerboseLogs -and $Meta) { $line += " | " + ($Meta | ConvertTo-Json -Compress | Out-String) }

  [System.Threading.Monitor]::Enter($Script:LogLock)
  try { Write-Host $line -ForegroundColor $color } finally { [System.Threading.Monitor]::Exit($Script:LogLock) }
}

# ---------- JSON helpers ----------
function Convert-PSCustomObjectToHashtable {
  param([pscustomobject]$Obj)
  $ht = @{}
  foreach ($p in $Obj.PSObject.Properties) {
    if ($p.Value -is [pscustomobject]) {
      $ht[$p.Name] = Convert-PSCustomObjectToHashtable $p.Value
    } elseif ($p.Value -is [System.Collections.IEnumerable] -and -not ($p.Value -is [string])) {
      $list = New-Object System.Collections.ArrayList
      foreach ($i in $p.Value) { if ($i -is [pscustomobject]) { [void]$list.Add((Convert-PSCustomObjectToHashtable $i)) } else { [void]$list.Add($i) } }
      $ht[$p.Name] = $list
    } else { $ht[$p.Name] = $p.Value }
  }
  $ht
}
function Load-JsonAsHashtable {
  param([string]$Path)
  if (-not (Test-Path $Path) -or (Get-Item $Path).Length -eq 0) { return @{} }
  try {
    $raw = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($raw)) { return @{} }
    $obj = $raw | ConvertFrom-Json -ErrorAction Stop
    if ($obj -is [hashtable]) { return $obj }
    if ($obj -is [pscustomobject]) { return Convert-PSCustomObjectToHashtable $obj }
    return @{ value = $obj }
  } catch {
    Write-Log WARN ("Ошибка чтения {0}: {1}" -f $Path, $_.Exception.Message)
    return @{}
  }
}
$RecoveredIndex = Load-JsonAsHashtable -Path $IndexPath

# ---------- Load caches ----------
$ValidPairs = New-Object System.Collections.Generic.HashSet[string]
if (Test-Path $ValidPairsPath) {
  Get-Content $ValidPairsPath | ForEach-Object {
    $line=$_.Trim()
    if ($line) { try { $obj=$line|ConvertFrom-Json; [void]$ValidPairs.Add(($obj.mp4_header + '||' + $obj.mov_fragment)) } catch {} }
  }
}
$FailedPairs = New-Object System.Collections.Generic.HashSet[string]
if (Test-Path $FailedPairsPath) {
  Get-Content $FailedPairsPath | ForEach-Object {
    $line=$_.Trim()
    if ($line) { try { $obj=$line|ConvertFrom-Json; [void]$FailedPairs.Add(($obj.mp4_header + '||' + $obj.mov_fragment)) } catch {} }
  }
}
$InvalidHeaders = New-Object System.Collections.Generic.HashSet[string]
if (Test-Path $InvalidHeadersPath) {
  Get-Content $InvalidHeadersPath | ForEach-Object {
    $line=$_.Trim()
    if ($line) { try { $obj=$line|ConvertFrom-Json; [void]$InvalidHeaders.Add($obj.mp4_header) } catch {} }
  }
}

function Save-ValidPair {
  param([string]$Mp4, [string]$Mov, [string]$Output, [hashtable]$Meta)
  $obj = [ordered]@{ timestamp=(Get-Date).ToString("o"); mp4_header=$Mp4; mov_fragment=$Mov; output=$Output; meta=$Meta }
  ($obj | ConvertTo-Json -Compress) | Add-Content -Path $ValidPairsPath -Encoding UTF8
  [void]$ValidPairs.Add($Mp4 + '||' + $Mov)
  if (-not $RecoveredIndex.ContainsKey($Mp4)) { $RecoveredIndex[$Mp4] = @{} }
  if (-not ($RecoveredIndex[$Mp4] -is [hashtable])) { $RecoveredIndex[$Mp4] = @{} + $RecoveredIndex[$Mp4] }
  $RecoveredIndex[$Mp4][$Mov] = $Output
  ($RecoveredIndex | ConvertTo-Json -Depth 10) | Set-Content -Path $IndexPath -Encoding UTF8
}
function Save-FailedPair { param([string]$Mp4, [string]$Mov, [string]$Reason)
  $obj = [ordered]@{ timestamp=(Get-Date).ToString("o"); mp4_header=$Mp4; mov_fragment=$Mov; reason=$Reason }
  ($obj | ConvertTo-Json -Compress) | Add-Content -Path $FailedPairsPath -Encoding UTF8
  [void]$FailedPairs.Add($Mp4 + '||' + $Mov)
}
function Save-InvalidHeader { param([string]$Mp4, [string]$Reason)
  $obj = [ordered]@{ timestamp=(Get-Date).ToString("o"); mp4_header=$Mp4; reason=$Reason }
  ($obj | ConvertTo-Json -Compress) | Add-Content -Path $InvalidHeadersPath -Encoding UTF8
  [void]$InvalidHeaders.Add($Mp4)
}
function Append-SummaryCsv {
  param(
    [string]$Output, [string]$Mp4, [string]$Mov, [long]$SizeBytes,
    [string]$Duration, [string]$VCodec, [string]$ACodec, [string]$AvgFps,
    [int]$ConcatMs, [int]$ValidateMs, [double]$Bps
  )
  $ts = (Get-Date).ToString("o")
  $vals = @(
    $ts, ($Output -replace ',','_'), ($Mp4 -replace ',','_'), ($Mov -replace ',','_'),
    $SizeBytes, $Duration, $VCodec, $ACodec, $AvgFps, $ConcatMs, $ValidateMs, [math]::Round($Bps,2)
  )
  $line = '{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}' -f $vals
  Add-Content -Path $SummaryCsvPath -Value $line -Encoding UTF8
}

# ---------- Scan inputs (exclude tmp/output/hidden/system) ----------
function Is-IgnoredPath {
  param([System.IO.FileInfo]$f, [string]$OutputDir, [string]$StateDir)
  if (-not $f) { return $true }
  # Skip hidden/system files
  if ($f.Attributes.HasFlag([System.IO.FileAttributes]::Hidden) -or
      $f.Attributes.HasFlag([System.IO.FileAttributes]::System)) { return $true }
  # Skip inside output/state directories
  $f.FullName.StartsWith((Resolve-Path $OutputDir).Path, [System.StringComparison]::OrdinalIgnoreCase) -or
  $f.FullName.StartsWith((Resolve-Path $StateDir).Path,  [System.StringComparison]::OrdinalIgnoreCase)
}
function Is-TempArtifact {
  param([System.IO.FileInfo]$f)
  $n = $f.Name.ToLowerInvariant()
  return ($n -like '*.tmp' -or $n -like '*.tmp.mp4' -or $n -like '*.part' -or $n -like '*.partial')
}
function Get-MediaFiles {
  param([string[]]$Roots, [switch]$Recurse)
  $all = New-Object System.Collections.Generic.List[System.IO.FileInfo]
  foreach ($r in $Roots) {
    if (-not (Test-Path -LiteralPath $r)) { Write-Log SKIP "Пропуск пути (не найден): $r"; continue }
    $item = Get-Item -LiteralPath $r
    if ($item.PSIsContainer) {
      $files = Get-ChildItem -LiteralPath $item.FullName -File -Recurse:$Recurse
      foreach ($f in $files) {
        if (Is-TempArtifact $f) { continue }
        if (Is-IgnoredPath -f $f -OutputDir $OutputDir -StateDir $StateDir) { continue }
        $all.Add($f) | Out-Null
      }
    } else {
      if ($item -is [System.IO.FileInfo]) {
        $f = [System.IO.FileInfo]$item
        if (-not (Is-TempArtifact $f) -and -not (Is-IgnoredPath -f $f -OutputDir $OutputDir -StateDir $StateDir)) { $all.Add($f) | Out-Null }
      }
    }
  }
  return $all | Where-Object { ($_.Extension -ieq '.mp4') -or ($_.Extension -ieq '.mov') }
}

$files = Get-MediaFiles -Roots $Paths -Recurse:$Recurse
if (-not $files -or $files.Count -eq 0) { Write-Log ERROR "Не найдено .mp4/.mov по указанным путям."; exit 2 }

$mp4s = $files | Where-Object { $_.Extension -ieq '.mp4' }
$movs = $files | Where-Object { $_.Extension -ieq '.mov' }

# ---------- Already recovered/used (from outputs and index) ----------
$RecoveredMp4Base = New-Object System.Collections.Generic.HashSet[string]
$UsedMovBase      = New-Object System.Collections.Generic.HashSet[string]

Get-ChildItem -LiteralPath $OutputDir -Filter "*.mp4" -File -ErrorAction SilentlyContinue | ForEach-Object {
  $bn = $_.BaseName
  $parts = $bn -split '__', 2
  if ($parts.Count -eq 2) { [void]$RecoveredMp4Base.Add($parts[0]); [void]$UsedMovBase.Add($parts[1]) }
}
$UsedMovs = New-Object System.Collections.Generic.HashSet[string]
foreach ($mp4Key in $RecoveredIndex.Keys) {
  $movMap = $RecoveredIndex[$mp4Key]
  if ($movMap -and ($movMap -is [hashtable])) {
    foreach ($mvKey in $movMap.Keys) {
      [void]$UsedMovs.Add($mvKey)
      $mvBase = Split-Path $mvKey -LeafBase
      if ($mvBase) { [void]$UsedMovBase.Add($mvBase) }
    }
    $mp4Base = Split-Path $mp4Key -LeafBase
    if ($mp4Base) { [void]$RecoveredMp4Base.Add($mp4Base) }
  }
}

# ---------- Header validation (always ftyp + ffprobe) ----------
function Has-Ftyp {
  param([System.IO.FileInfo]$File)
  $len = [Math]::Min($File.Length, 131072)
  $fs = $File.OpenRead()
  try {
    $buf = New-Object byte[] $len
    [void]$fs.Read($buf,0,$len)
    $text = [System.Text.Encoding]::ASCII.GetString($buf)
    return ($text.Contains("ftyp"))
  } finally { $fs.Dispose() }
}
function Test-Mp4Header {
  param([System.IO.FileInfo]$HeaderFile)
  if (-not (Has-Ftyp -File $HeaderFile)) { return @{ ok=$false; reason='no_ftyp' } }
  $null = & $ffprobe -v error -hide_banner -show_entries format=format_name -of json -- "$($HeaderFile.FullName)" 2>$null
  if ($LASTEXITCODE -ne 0) { return @{ ok=$false; reason=("ffprobe_header_exit_{0}" -f $LASTEXITCODE) } }
  return @{ ok=$true; reason='ok' }
}

# Filter by state (unless ForceRecheck)
if (-not $ForceRecheck) {
  $mp4s = $mp4s | Where-Object { -not $RecoveredIndex.ContainsKey($_.FullName) -and -not $RecoveredMp4Base.Contains($_.BaseName) }
  $movs = $movs | Where-Object  { -not $UsedMovs.Contains($_.FullName) -and -not $UsedMovBase.Contains($_.BaseName) }
}
# Exclude known invalid headers
$mp4s = $mp4s | Where-Object { -not $InvalidHeaders.Contains($_.FullName) }

# Strict header check
$validatedMp4s = New-Object System.Collections.Generic.List[System.IO.FileInfo]
foreach ($h in $mp4s) {
  $hdr = Test-Mp4Header -HeaderFile $h
  if (-not $hdr.ok) {
    Write-Log SKIP ("Невалидный MP4-хедер: {0} ({1})" -f $h.Name, $hdr.reason)
    Save-InvalidHeader -Mp4 $h.FullName -Reason $hdr.reason
  } else {
    $validatedMp4s.Add($h) | Out-Null
  }
}
$mp4s = $validatedMp4s

# ---------- Sorting ----------
$mp4sByName = $mp4s | Sort-Object BaseName, Length, FullName
$movsByName = $movs | Sort-Object BaseName, Length, FullName
$mp4sBySize = $mp4s | Sort-Object Length, FullName
$movsBySize = $movs | Sort-Object Length, FullName

Write-Log STAT ("Найдено: MP4-хедеров {0}, MOV-фрагментов {1}" -f $mp4s.Count, $movs.Count)
if ($mp4s.Count -eq 0 -or $movs.Count -eq 0) { Write-Log ERROR "Недостаточно файлов (нужны .mp4 и .mov)."; exit 3 }

# ---------- Pairing ----------
function Extract-Digits { param([string]$s) $m = ([regex]'\d+').Matches($s) | ForEach-Object { $_.Value }; return ($m -join '_') }
function Extract-Offset {
  param([string]$name)
  if ($name -match '^f(\d+)') { return [int64]$matches[1] }
  return $null
}
function Take-Top {
  param([object[]]$arr, [int]$n)
  if (-not $arr) { return @() }
  $arr = @($arr)
  $count = [Math]::Min($n, $arr.Count)
  if ($count -le 0) { return @() }
  return $arr[0..($count-1)]
}

function Build-Candidates {
  param(
    [System.IO.FileInfo[]]$Mp4sByName,
    [System.IO.FileInfo[]]$MovsByName,
    [System.IO.FileInfo[]]$Mp4sBySize,
    [System.IO.FileInfo[]]$MovsBySize,
    [string]$Mode,
    [long]$NearBytes,
    [int]$TopN
  )
  $pairs = New-Object System.Collections.Generic.List[object]
  $seen  = New-Object System.Collections.Generic.HashSet[string]

  # Index MOV by offset and digits
  $movByOffset = @{}
  $movByDigits = @{}
  foreach ($m in $MovsByName) {
    $mo = Extract-Offset $m.BaseName
    if ($mo -ne $null) {
      if (-not $movByOffset.ContainsKey($mo)) { $movByOffset[$mo] = New-Object System.Collections.Generic.List[object] }
      $movByOffset[$mo].Add($m)
    }
    $d = Extract-Digits -s $m.BaseName
    if (-not $movByDigits.ContainsKey($d)) { $movByDigits[$d] = New-Object System.Collections.Generic.List[object] }
    $movByDigits[$d].Add($m)
  }

  # pr=0 exact offset match
  foreach ($ph in $Mp4sByName) {
    $po = Extract-Offset $ph.BaseName
    if ($po -ne $null -and $movByOffset.ContainsKey($po)) {
      foreach ($mv in $movByOffset[$po]) {
        $key = $ph.FullName + '||' + $mv.FullName
        if ($seen.Add($key)) { $pairs.Add([pscustomobject]@{ mp4=$ph; mov=$mv; pr=0; score=0 }) }
      }
    }
  }

  # pr=1 near offsets (header before mdat, within NearBytes)
  foreach ($ph in $Mp4sByName) {
    $po = Extract-Offset $ph.BaseName
    if ($po -eq $null) { continue }
    foreach ($entry in $movByOffset.GetEnumerator()) {
      $mo = [int64]$entry.Key
      $delta = $mo - $po
      if ($delta -ge 0 -and $delta -le $NearBytes) {
        foreach ($mv in $entry.Value) {
          $key = $ph.FullName + '||' + $mv.FullName
          if ($seen.Add($key)) { $pairs.Add([pscustomobject]@{ mp4=$ph; mov=$mv; pr=1; score=$delta }) }
        }
      }
    }
  }

  # pr=2 name/digits/prefix + nearest-by-size
  if ($Mode -in @('name','mixed')) {
    foreach ($ph in $Mp4sByName) {
      $d = Extract-Digits -s $ph.BaseName
      $cands = @()
      if ($d -and $movByDigits.ContainsKey($d)) {
        $cands = $movByDigits[$d]
      } else {
        $prefix = ($ph.BaseName -split '[^A-Za-zА-Яа-я0-9]+')[0]
        if ($prefix) {
          $cands = $MovsByName | Where-Object { $_.BaseName.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase) }
        }
      }
      $nearBySize = Take-Top -arr ($MovsBySize | Sort-Object @{Expression={ [math]::Abs($_.Length - $ph.Length) }}) -n $TopN
      $proposal = @($cands + $nearBySize) | Select-Object -Unique
      foreach ($mv in $proposal) {
        $key = $ph.FullName + '||' + $mv.FullName
        if ($seen.Add($key)) { $pairs.Add([pscustomobject]@{ mp4=$ph; mov=$mv; pr=2; score=[int64][math]::Abs($mv.Length - $ph.Length) }) }
      }
    }
  }

  # pr=3 nearest-by-size only
  if ($Mode -in @('size','mixed')) {
    foreach ($ph in $Mp4sBySize) {
      $closest = Take-Top -arr ($MovsBySize | Sort-Object @{Expression={ [math]::Abs($_.Length - $ph.Length) }}) -n $TopN
      foreach ($mv in $closest) {
        $key = $ph.FullName + '||' + $mv.FullName
        if ($seen.Add($key)) { $pairs.Add([pscustomobject]@{ mp4=$ph; mov=$mv; pr=3; score=[int64][math]::Abs($mv.Length - $ph.Length) }) }
      }
    }
  }

  $pairs | Sort-Object @{Expression='pr'}, @{Expression='score'}, @{Expression={ $_.mp4.BaseName }}
}

$candidates = Build-Candidates -Mp4sByName $mp4sByName -MovsByName $movsByName -Mp4sBySize $mp4sBySize -MovsBySize $movsBySize -Mode $PairingMode -NearBytes $OffsetNearBytes -TopN $SizeTopN
Write-Log STAT ("Сформировано кандидатных пар: {0}" -f $candidates.Count)
if ($candidates.Count -eq 0) { Write-Log WARN "Кандидатов нет после фильтрации."; exit 0 }

# ---------- Disk safety ----------
function Get-FreeGB {
  param([string]$Path)
  $root = (Get-Item -LiteralPath $Path).PSDrive.Root
  $drive = Get-PSDrive | Where-Object { $_.Root -eq $root } | Select-Object -First 1
  if (-not $drive) { return 0 }
  [math]::Round($drive.Free/1GB,3)
}
function Ensure-FreeSpace {
  param([long]$MovBytes, [long]$HeaderBytes, [double]$MinFreeGB, [string]$OutDir)
  $free = Get-FreeGB -Path $OutDir
  $needGB = if ($MinFreeGB -gt 0) { $MinFreeGB } else { (($MovBytes + $HeaderBytes) * 1.24) / 1GB }
  if ($free -lt $needGB) { throw ("Недостаточно места. Доступно: {0} GB, минимум: {1} GB" -f ([math]::Round($free,3)), [math]::Round($needGB,3)) }
  $free
}

# ---------- Concat with progress ----------

function Concat-WithProgress {
  param(
    [Parameter(Mandatory=$true)][System.IO.FileInfo]$Header,
    [Parameter(Mandatory=$true)][System.IO.FileInfo]$Mov,
    [string]$TempOut,
    [int]$PairIndex,
    [int]$PairTotal
  )

  if (-not $Header.Exists) { throw "Header file not found: $($Header.FullName)" }
  if (-not $Mov.Exists)    { throw "Mov file not found:   $($Mov.FullName)" }

  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $total = $Header.Length + $Mov.Length
  $bufferSize = 24MB
  $written = 0L

  $out = [System.IO.File]::Open($TempOut, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
  try {
    foreach ($part in @($Header, $Mov)) {
      try {
        if (-not $part -or -not $part.Exists) {
          Write-Log ERROR ("Файл отсутствует: {0}" -f $part?.FullName)
          throw "concat_error_file_missing"
        }
        $in = $part.OpenRead()
        try {
          $buf = New-Object byte[] $bufferSize
          while ($true) {
            $read = $in.Read($buf, 0, $buf.Length)
            if ($read -le 0) { break }

            try {
              $out.Write($buf, 0, $read)
            } catch {
              Write-Log ERROR ("Ошибка записи: {0}" -f $_.Exception.Message)
              throw "concat_error_write_failed"
            }

            $written += $read
            $elapsed = $sw.Elapsed.TotalSeconds
            $speed = if ($elapsed -gt 0) { $written / $elapsed } else { 0 }
            $percent = if ($total -gt 0) { [int](($written * 100) / $total) } else { 0 }
            $remaining = if ($speed -gt 0) { ($total - $written) / $speed } else { 0 }

            $overall = ((($PairIndex - 1) / [double]$PairTotal) * 100.0) + ($percent / [double]$PairTotal)
            Write-Progress -Id 1 -Activity "Обработка пар ($PairIndex/$PairTotal)" -Status "Склейка: $($Header.BaseName)__$($Mov.BaseName).mp4" -PercentComplete $overall
            Write-Progress -Id 2 -ParentId 1 -Activity "Склейка фрагментов" -Status ("{0}% | {1:N2} MB из {2:N2} MB | {3:N2} MB/s | ETA {4:N1}s" -f $percent, ($written/1MB), ($total/1MB), ($speed/1MB), $remaining) -PercentComplete $percent
          }
        } finally {
          $in.Dispose()
        }
      } catch {
        Write-Log ERROR ("Ошибка открытия или чтения файла: {0} |err>| {1}" -f $part.FullName, $_.Exception.Message)
        throw "concat_error_open_failed"
      }
    }
  } finally {
    $out.Flush(); $out.Dispose()
    Write-Progress -Id 2 -ParentId 1 -Activity "Склейка фрагментов" -Completed
  }
  $sw.Stop()
  $bps = if ($sw.Elapsed.TotalSeconds -gt 0) { $written / $sw.Elapsed.TotalSeconds } else { 0 }
  @{ ms=[int]$sw.Elapsed.TotalMilliseconds; bytes=$written; bps=$bps }
}

# ---------- Validation ----------
function Validate-Output {
  param(
    [Parameter(Mandatory=$true)][string]$Path,
    [int]$FPS = 25
  )

  $ffprobe = "ffprobe"
  $ffmpeg  = "ffmpeg"
  $sw = [System.Diagnostics.Stopwatch]::StartNew()

  if (-not (Test-Path $Path)) {
    return @{ ok=$false; ms=0; reason="file_not_found"; path=$Path }
  }

  # --- ffprobe ---
  $probeJson = & $ffprobe -v error -hide_banner `
    -select_streams v:0 `
    -show_entries stream=codec_name,codec_type,width,height,avg_frame_rate `
    -show_entries format=duration `
    -of json -- "$Path" 2>$null
  $probeExit = $LASTEXITCODE

  if ($probeExit -ne 0) {
    $sw.Stop()
    return @{ ok=$false; ms=[int]$sw.Elapsed.TotalMilliseconds; reason="ffprobe_exit_$probeExit"; path=$Path }
  }

  $meta = $null
  try { $meta = $probeJson | ConvertFrom-Json } catch {
    $sw.Stop()
    return @{ ok=$false; ms=[int]$sw.Elapsed.TotalMilliseconds; reason="ffprobe_json_error"; path=$Path }
  }

  # --- ffmpeg decode ---
  $null = & $ffmpeg -v error -xerror -hide_banner -nostdin `
    -i "$Path" -frames:v $FPS -f null NUL 2>$null
  $ffExit = $LASTEXITCODE
  $sw.Stop()

  if ($ffExit -ne 0) {
    return @{
      ok      = $false
      ms      = [int]$sw.Elapsed.TotalMilliseconds
      reason  = "ffmpeg_decode_exit_$ffExit"
      path    = $Path
      meta    = $meta
    }
  }

  # --- Extract metadata ---
  $duration = ''; $vcodec = ''; $acodec = ''; $avgfps = ''
  try {
    $videoStream = $meta.streams | Where-Object { $_.codec_type -eq 'video' } | Select-Object -First 1
    $audioStream = $meta.streams | Where-Object { $_.codec_type -eq 'audio' } | Select-Object -First 1

    $duration = if ($meta.format.duration) { "{0:N3}" -f ([double]$meta.format.duration) } else { "0.000" }
    $vcodec   = $videoStream?.codec_name
    $acodec   = $audioStream?.codec_name
    $avgfps   = $videoStream?.avg_frame_rate
  } catch {}

  return @{
    ok        = $true
    ms        = [int]$sw.Elapsed.TotalMilliseconds
    path      = $Path
    duration  = $duration
    vcodec    = $vcodec
    acodec    = $acodec
    avgfps    = $avgfps
    meta      = $meta
  }
}


# !---------{ MAIN LOOP }---------!
$skipCounters = @{
  validCache     = 0
  failedCache    = 0
  mp4Recovered   = 0
  movUsed        = 0
  fileExists     = 0
  noSpace        = 0
  dryRun         = 0
}

$restoredCount = 0
$attempt = 0

$candidates = $candidates  # already built earlier
$sessionPairsTotal = $candidates.Count
$sessionStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

foreach ($pair in $candidates) {
  $attempt++

  $ph  = $pair.mp4
  $mv  = $pair.mov
  $key = $ph.FullName + '||' + $mv.FullName

  # Skips with reasons
  if (-not $ForceRecheck -and $ValidPairs.Contains($key)) { $skipCounters.validCache++; Write-Log SKIP ("Пропуск: уже валидная пара (кэш): {0} + {1}" -f $ph.Name, $mv.Name); continue }
  if ($FailedPairs.Contains($key)) { $skipCounters.failedCache++; Write-Log SKIP ("Пропуск: пара уже была неуспешной: {0} + {1}" -f $ph.Name, $mv.Name); continue }
  if (-not $ForceRecheck) {
    if ($RecoveredIndex.ContainsKey($ph.FullName) -or $RecoveredMp4Base.Contains($ph.BaseName)) { $skipCounters.mp4Recovered++; Write-Log SKIP ("Пропуск: MP4 уже восстановлен: {0}" -f $ph.Name); continue }
    if ($UsedMovs.Contains($mv.FullName) -or $UsedMovBase.Contains($mv.BaseName)) { $skipCounters.movUsed++; Write-Log SKIP ("Пропуск: MOV уже использован: {0}" -f $mv.Name); continue }
  }

  $outNameBase = ("{0}__{1}" -f $ph.BaseName, $mv.BaseName)
  $finalOut = Join-Path $OutputDir ($outNameBase + ".mp4")
  $tempOut  = Join-Path $OutputDir ($outNameBase + ".tmp.mp4")

  if (-not $ForceRecheck -and (Test-Path $finalOut)) { $skipCounters.fileExists++; Write-Log SKIP ("Пропуск: итоговый уже существует: {0}" -f (Split-Path $finalOut -Leaf)); continue }

  Write-Log INFO ("[{0}/{1}] Пробуем: {2} + {3}" -f $attempt, $sessionPairsTotal, $ph.Name, $mv.Name) @{
    pr=$pair.pr; score=$pair.score; headerOffset=(Extract-Offset $ph.BaseName); movOffset=(Extract-Offset $mv.BaseName); headerSize=$ph.Length; movSize=$mv.Length
  }

  # Space check
  try {
    $freeGB = Ensure-FreeSpace -MovBytes $mv.Length -HeaderBytes $ph.Length -MinFreeGB $MinFreeGB -OutDir $OutputDir
    Write-Log INFO ("Свободно: {0} GB" -f ([math]::Round($freeGB,3)))
  } catch {
    $skipCounters.noSpace++
    Write-Log WARN $_.Exception.Message
    Save-FailedPair -Mp4 $ph.FullName -Mov $mv.FullName -Reason "no_space"
    continue
  }

  if ($DryRun) { $skipCounters.dryRun++; Write-Log SKIP "DryRun: конкатенация пропущена"; continue }

  if (Test-Path $tempOut) { Remove-Item -LiteralPath $tempOut -Force -ErrorAction SilentlyContinue }
  if (Test-Path $finalOut) { Remove-Item -LiteralPath $finalOut -Force -ErrorAction SilentlyContinue }

  # Concat
  $concatStats = $null
  try {
    $concatStats = Concat-WithProgress -Header $ph -Mov $mv -TempOut $tempOut -PairIndex $attempt -PairTotal $sessionPairsTotal
  } catch {
    $reason = $_.Exception.Message
    Write-Log ERROR ("Ошибка склейки: {0}" -f $reason)
    Save-FailedPair -Mp4 $ph.FullName -Mov $mv.FullName -Reason "concat_error: $reason"
    continue
  }

  # Validate
  $val = Validate-Output -Path $tempOut -FPS $FPS
  if (-not $val.ok) {
    Write-Log WARN ("Невалидный результат: {0}" -f $val.reason)
    Save-FailedPair -Mp4 $ph.FullName -Mov $mv.FullName -Reason $val.reason
    Remove-Item -LiteralPath $tempOut -Force -ErrorAction SilentlyContinue
    continue
  }

  # Finalize
  try { Move-Item -LiteralPath $tempOut -Destination $finalOut -Force }
  catch {
    Write-Log ERROR ("Не удалось сохранить итоговый файл: {0}" -f $_.Exception.Message)
    Save-FailedPair -Mp4 $ph.FullName -Mov $mv.FullName -Reason "move_error"
    if (Test-Path $tempOut) { Remove-Item -LiteralPath $tempOut -Force -ErrorAction SilentlyContinue }
    continue
  }

  $restoredCount++
  Write-Log OK ("VALID ✓ {0}" -f (Split-Path $finalOut -Leaf)) @{
    duration=$val.duration; vcodec=$val.vcodec; acodec=$val.acodec; fps=$val.avgfps; sizeBytes=$concatStats.bytes
  }

  $meta = @{
    duration      = $val.duration
    video_codec   = $val.vcodec
    audio_codec   = $val.acodec
    avg_fps       = $val.avgfps
    concat_ms     = $concatStats.ms
    validate_ms   = $val.ms
    bytes         = $concatStats.bytes
    bytes_per_sec = [math]::Round($concatStats.bps,2)
  }
  Save-ValidPair -Mp4 $ph.FullName -Mov $mv.FullName -Output $finalOut -Meta $meta
  Append-SummaryCsv -Output $finalOut -Mp4 $ph.FullName -Mov $mv.FullName -SizeBytes $concatStats.bytes -Duration $val.duration -VCodec $val.vcodec -ACodec $val.acodec -AvgFps $val.avgfps -ConcatMs $meta.concat_ms -ValidateMs $meta.validate_ms -Bps $meta.bytes_per_sec

  $null = $UsedMovs.Add($mv.FullName)
  $null = $UsedMovBase.Add($mv.BaseName)
  $null = $RecoveredMp4Base.Add($ph.BaseName)

  if ($MaxPairs -gt 0 -and $restoredCount -ge $MaxPairs) {
    Write-Log STAT ("Достигнут лимит валидных: {0}. Завершение." -f $MaxPairs)
    break
  }
}

$sessionStopwatch.Stop()
Write-Progress -Id 1 -Activity "Обработка пар" -Completed
Write-Log STAT ("Готово. Валидных: {0}. Время: {1:N2}s" -f $restoredCount, $sessionStopwatch.Elapsed.TotalSeconds)
Write-Log STAT ("Пропуски: validCache={0}, failedCache={1}, mp4Recovered={2}, movUsed={3}, fileExists={4}, noSpace={5}, dryRun={6}" -f `
  $skipCounters.validCache, $skipCounters.failedCache, $skipCounters.mp4Recovered, $skipCounters.movUsed, $skipCounters.fileExists, $skipCounters.noSpace, $skipCounters.dryRun)
