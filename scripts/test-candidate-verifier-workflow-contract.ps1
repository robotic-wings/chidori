[CmdletBinding()]
param([switch]$FixturesOnly)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    try { & $Action } catch { return }
    throw $Message
}

function Git([string]$Repo, [string[]]$Arguments) {
    $out = & git.exe -C $Repo @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) { throw "git $($Arguments -join ' ') failed" }
    return ($out | Out-String).TrimEnd()
}

function Require-CandidateSha([string]$CandidateSha) {
    if ($CandidateSha -cnotmatch '\A[0-9a-f]{40}\z') {
        throw 'candidate_sha must be exactly 40 lowercase hexadecimal characters'
    }
}

function Assert-InvalidCandidateSha([string]$CandidateSha) {
    try { Require-CandidateSha $CandidateSha } catch { return }
    throw "must reject malformed candidate input [$CandidateSha]"
}

function Assert-CandidateCheckout(
    [string]$Repo,
    [string]$CandidateSha,
    [string]$HeadSha,
    [string]$TrustedVerifierSha,
    [string]$VerifierSha,
    [string]$BaseSha,
    [string]$Ref
) {
    Require-CandidateSha $CandidateSha
    Assert ($Ref -eq 'refs/heads/main') 'workflow must run only for refs/heads/main'
    Assert ($CandidateSha -eq $HeadSha) 'candidate checkout HEAD must equal candidate_sha'
    Assert ($VerifierSha -eq $TrustedVerifierSha) 'trusted verifier checkout must equal github.sha'
    Assert ($VerifierSha -ne $CandidateSha) 'trusted verifier and candidate identities must remain separate'
    Assert ((Git $Repo @('rev-parse', "$CandidateSha^{commit}")) -eq $CandidateSha) 'candidate SHA must be fetchable'
    $parents = (Git $Repo @('show', '-s', '--format=%P', $CandidateSha)).Split(' ', [StringSplitOptions]::RemoveEmptyEntries)
    Assert ($parents.Count -eq 1) 'candidate must have exactly one parent'
    Assert ($parents[0] -eq $BaseSha) 'candidate parent must equal approved base'
}

function Assert-WorkflowContract([string]$Workflow) {
    Assert (Test-Path -LiteralPath $Workflow) 'missing .github/workflows/candidate-verifier.yml'
    $text = [IO.File]::ReadAllText($Workflow)
    Assert ($text -match '(?ms)^on:\s*\r?\n\s*workflow_dispatch:\s*$') 'workflow must be workflow_dispatch-only'
    Assert ($text -notmatch '(?m)^\s*(pull_request|push|schedule):') 'workflow must not have automatic triggers'
    Assert ($text -match 'github\.ref\s*==\s*[''\"]refs/heads/main[''\"]') 'workflow must guard main ref'
    Assert ($text -match '(?ms)^permissions:\s*\r?\n\s*contents:\s*read\s*$') 'workflow must grant only contents: read'
    Assert ($text -match 'persist-credentials:\s*false') 'checkout must disable persisted credentials'
    Assert ($text -notmatch '(?i)(secrets\.|github\.token|GH_TOKEN)') 'workflow must not expose secrets or token variables'
    Assert ($text -match 'CANDIDATE_SHA') 'workflow must assign candidate SHA to CANDIDATE_SHA'
    Assert ($text -notmatch '\$\{\{\s*inputs\.candidate_sha\s*\}\}') 'workflow must not interpolate input SHA into shell or refs'
    Assert ($text -match '\^\[0-9a-f\]\{40\}\$') 'workflow must validate exact lowercase 40-hex SHA before use'
    Assert ($text -match 'git worktree add --detach') 'workflow must use detached candidate checkout'
    Assert ($text -match 'git status --porcelain') 'workflow must require clean tracked trees'
    Assert ($text -match 'verify-package-candidate\.ps1') 'workflow must invoke trusted verifier script'
    Assert ($text -match 'cargo metadata --locked') 'workflow must require candidate locked metadata evidence'
    Assert ($text -match '(?i)(env -i|Remove-Item Env:|GIT_CONFIG_NOSYSTEM)') 'workflow must scrub candidate execution environment'
}

function New-FixtureRepo {
    $repo = Join-Path ([IO.Path]::GetTempPath()) ("chidori-workflow-contract-" + [Guid]::NewGuid())
    New-Item -ItemType Directory -Path $repo | Out-Null
    Git $repo @('init', '--initial-branch=main') | Out-Null
    Git $repo @('config', 'user.email', 'contract@example.invalid') | Out-Null
    Git $repo @('config', 'user.name', 'workflow-contract') | Out-Null
    [IO.File]::WriteAllText((Join-Path $repo 'base.txt'), 'base')
    Git $repo @('add', 'base.txt') | Out-Null
    Git $repo @('commit', '-m', 'base') | Out-Null
    $base = Git $repo @('rev-parse', 'HEAD')
    [IO.File]::WriteAllText((Join-Path $repo 'candidate.txt'), 'candidate')
    Git $repo @('add', 'candidate.txt') | Out-Null
    Git $repo @('commit', '-m', 'candidate') | Out-Null
    $candidate = Git $repo @('rev-parse', 'HEAD')
    Git $repo @('checkout', '-q', '--detach', $candidate) | Out-Null
    return @{ Repo = $repo; Base = $base; Candidate = $candidate }
}

function Invoke-Fixtures {
    $fixture = New-FixtureRepo
    try {
        # Happy path assembles a detached candidate/checker identity without
        # dispatching Actions or invoking Cargo.
        Assert-CandidateCheckout $fixture.Repo $fixture.Candidate $fixture.Candidate $fixture.Base $fixture.Base $fixture.Base 'refs/heads/main'
        foreach ($bad in @('main', 'abc', ('A' * 40), '', (' ' + $fixture.Candidate), ($fixture.Candidate + "`n"), ($fixture.Candidate + ';id'))) {
            Assert-InvalidCandidateSha $bad
        }
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo ('0' * 40) ('0' * 40) $fixture.Base $fixture.Base $fixture.Base 'refs/heads/main' } 'must reject missing SHA'
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo $fixture.Candidate $fixture.Base $fixture.Base $fixture.Base $fixture.Base 'refs/heads/main' } 'must reject candidate/HEAD mismatch'
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo $fixture.Candidate $fixture.Candidate $fixture.Base $fixture.Candidate $fixture.Base 'refs/heads/main' } 'must reject swapped verifier/candidate SHA'
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo $fixture.Candidate $fixture.Candidate $fixture.Base $fixture.Base $fixture.Base 'refs/heads/release' } 'must reject non-main ref'

        [IO.File]::WriteAllText((Join-Path $fixture.Repo 'other.txt'), 'other')
        Git $fixture.Repo @('add', 'other.txt') | Out-Null
        Git $fixture.Repo @('commit', '-m', 'moved') | Out-Null
        $moved = Git $fixture.Repo @('rev-parse', 'HEAD')
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo $moved $moved $fixture.Base $fixture.Base $fixture.Base 'refs/heads/main' } 'must reject moved candidate SHA'

        $tree = Git $fixture.Repo @('rev-parse', "$moved^{tree}")
        $merge = Git $fixture.Repo @('commit-tree', $tree, '-p', $fixture.Base, '-p', $moved, '-m', 'merge candidate')
        Assert-Throws { Assert-CandidateCheckout $fixture.Repo $merge $merge $fixture.Base $fixture.Base $fixture.Base 'refs/heads/main' } 'must reject merge candidate'
    } finally {
        if (Test-Path -LiteralPath $fixture.Repo) { Remove-Item -LiteralPath $fixture.Repo -Recurse -Force }
    }
}

Invoke-Fixtures
if (-not $FixturesOnly) {
    Assert-WorkflowContract (Join-Path $PSScriptRoot '..\.github\workflows\candidate-verifier.yml')
}
Write-Host 'PASS: candidate-verifier workflow contracts and hermetic fixtures'
