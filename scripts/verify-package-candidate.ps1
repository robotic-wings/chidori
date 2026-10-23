[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Candidate
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Git([string[]]$Arguments) {
    $out = & git.exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "git $($Arguments -join ' ') failed" }
    return ($out | Out-String).TrimEnd()
}

function Assert-Equal([string]$Actual, [string]$Expected, [string]$What) {
    if ($Actual -ne $Expected) { throw "$What expected $Expected, got $Actual" }
}

function Blob([string]$Revision, [string]$Path) {
    return Git @('rev-parse', "$Revision`:$Path")
}

function FileAt([string]$Revision, [string]$Path) {
    return Git @('show', "$Revision`:$Path")
}

# This is deliberately a single-commit input check, never a commit-range check.
$packageCommit = '59ebb109'
$ctutilsCommit = '00148aa'
$base = '9e1c295'
$paths = @('Cargo.lock', 'crates/chidori-js/Cargo.toml', 'crates/chidori/Cargo.toml')

$candidateSha = Git @('rev-parse', "$Candidate^{commit}")
$headSha = Git @('rev-parse', 'HEAD^{commit}')
Assert-Equal $candidateSha $headSha 'candidate must be the checked-out HEAD'
$null = Git @('merge-base', '--is-ancestor', $base, $candidateSha)

$dirtyBefore = Git @('status', '--porcelain', '--untracked-files=no')
if ($dirtyBefore) { throw "tracked tree is dirty before verification: $dirtyBefore" }

$candidatePaths = Git @('diff', '--name-only', $base, $candidateSha)
Assert-Equal $candidatePaths ($paths -join [Environment]::NewLine) 'candidate changed-path set from 9e1c295'

# Compare every base-tracked blob outside the three allowlisted package paths,
# rather than relying on an ancestry relationship or a summarized diff.
$baseTrackedPaths = Git @('ls-tree', '-r', '--name-only', $base)
foreach ($path in ($baseTrackedPaths -split [Environment]::NewLine)) {
    if ($path -and $paths -notcontains $path) {
        Assert-Equal (Blob $candidateSha $path) (Blob $base $path) "non-allowlisted base blob $path"
    }
}

$directPaths = Git @('show', '--format=', '--name-only', $packageCommit)
Assert-Equal $directPaths ($paths -join [Environment]::NewLine) '59ebb109 changed-path set'
Assert-Equal (Blob "$packageCommit^" 'Cargo.lock') '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9' 'package lock base blob'
Assert-Equal (Blob $packageCommit 'Cargo.lock') '9e36a2e89363eeedc4215a68dbd51f33dfec3741' 'package lock target blob'
Assert-Equal (Blob "$packageCommit^" 'crates/chidori-js/Cargo.toml') '6c1d66fe28dbe211452e5c04176083b9dbddbc0d' 'chidori-js base blob'
Assert-Equal (Blob $packageCommit 'crates/chidori-js/Cargo.toml') '3977de82f979562052ef5ee5a43260dd0857ba3d' 'chidori-js target blob'
Assert-Equal (Blob "$packageCommit^" 'crates/chidori/Cargo.toml') 'cee7ace2bf9f813086176aee084ec0b0867dfa67' 'chidori base blob'
Assert-Equal (Blob $packageCommit 'crates/chidori/Cargo.toml') 'fc56a8752c8cc0ba8fe3a3ba64cb2d841ab881d5' 'chidori target blob'

# Materialize the exact combination from the two direct patches in a disposable
# directory. Exact byte equality below proves all non-allowlisted lock stanzas,
# dependency lists, features, sources, and checksums were preserved.
$temp = Join-Path ([IO.Path]::GetTempPath()) ("chidori-package-gate-" + [Guid]::NewGuid())
try {
    New-Item -ItemType Directory -Path $temp | Out-Null
    $packagePatch = Join-Path $temp 'package.patch'
    $ctutilsPatch = Join-Path $temp 'ctutils.patch'
    # cmd.exe redirection is intentional: Windows PowerShell text pipelines
    # alter Git's raw bytes. These files must retain Git blob bytes exactly.
    & cmd.exe /d /s /c "git show $base`:Cargo.lock > `"$temp\Cargo.lock`""
    if ($LASTEXITCODE -ne 0) { throw 'base Cargo.lock materialization failed' }
    & cmd.exe /d /s /c "git diff --binary $packageCommit~1 $packageCommit -- Cargo.lock > `"$packagePatch`""
    if ($LASTEXITCODE -ne 0) { throw 'package patch materialization failed' }
    & cmd.exe /d /s /c "git diff --binary $ctutilsCommit~1 $ctutilsCommit -- Cargo.lock > `"$ctutilsPatch`""
    if ($LASTEXITCODE -ne 0) { throw 'ctutils patch materialization failed' }
    Push-Location $temp
    try {
        & git.exe apply --no-index $packagePatch
        if ($LASTEXITCODE -ne 0) { throw '59ebb109 direct patch did not apply to base' }
        & git.exe apply --no-index $ctutilsPatch
        if ($LASTEXITCODE -ne 0) { throw '00148aa direct ctutils patch did not apply after package patch' }
    } finally {
        Pop-Location
    }

    Assert-Equal (Git @('hash-object', (Join-Path $temp 'Cargo.lock'))) (Blob $candidateSha 'Cargo.lock') 'candidate Cargo.lock blob'
    Assert-Equal (Blob $packageCommit 'crates/chidori-js/Cargo.toml') (Blob $candidateSha 'crates/chidori-js/Cargo.toml') 'candidate chidori-js manifest blob'
    Assert-Equal (Blob $packageCommit 'crates/chidori/Cargo.toml') (Blob $candidateSha 'crates/chidori/Cargo.toml') 'candidate chidori manifest blob'
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
}

$lock = FileAt $candidateSha 'Cargo.lock'
if ($lock -notmatch '(?s)name = "ctutils"\s*version = "0\.4\.4"\s*source = "registry\+https://github\.com/rust-lang/crates\.io-index"\s*checksum = "12218662ccac37d9714f2191252553d67585b97df8c839cbe49e0bde278dc865"') {
    throw 'candidate Cargo.lock lacks the exact ctutils 0.4.4 checksum stanza'
}
$jsManifest = FileAt $candidateSha 'crates/chidori-js/Cargo.toml'
$chidoriManifest = FileAt $candidateSha 'crates/chidori/Cargo.toml'
if ($jsManifest -notmatch '(?m)^version = "0\.4\.1"$') { throw 'chidori-js manifest is not exactly version 0.4.1' }
if ($chidoriManifest -notmatch '(?m)^chidori-js = \{ path = "\.\./chidori-js", version = "0\.4\.1" \}$') { throw 'chidori path dependency is not exact chidori-js 0.4.1' }

& cargo metadata --locked --format-version 1 --no-deps | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'cargo metadata --locked failed' }
& cargo tree --locked -p chidori | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'cargo tree --locked -p chidori failed' }

$dirtyAfter = Git @('status', '--porcelain', '--untracked-files=no')
if ($dirtyAfter) { throw "tracked tree is dirty after verification: $dirtyAfter" }
Write-Host "PASS: candidate $candidateSha is checked-out HEAD and is bound to direct patches 59ebb109 + 00148aa with locked metadata/tree evidence."
