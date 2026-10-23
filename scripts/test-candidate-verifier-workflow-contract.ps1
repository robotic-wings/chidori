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

function Assert-EnvBoundOperation([string]$Text, [string]$Name, [string]$Pattern) {
    $operations = [regex]::Matches($Text, $Pattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [Text.RegularExpressions.RegexOptions]::Multiline)
    Assert ($operations.Count -gt 0) "workflow must contain $Name"
    foreach ($operation in $operations) {
        $start = [Math]::Max(0, $operation.Index - 1200)
        $context = $Text.Substring($start, $operation.Index - $start + $operation.Length)
        Assert ($context -match '\$env:CANDIDATE_SHA') "$Name must be bound to `$env:CANDIDATE_SHA"
    }
}

function Assert-WorkflowContract([string]$Workflow) {
    Assert (Test-Path -LiteralPath $Workflow) 'missing .github/workflows/candidate-verifier.yml'
    $text = [IO.File]::ReadAllText($Workflow)
    Assert ($text -match '(?ms)^on:\s*\r?\n\s*workflow_dispatch:\s*$') 'workflow must be workflow_dispatch-only'
    Assert ($text -notmatch '(?m)^\s*(pull_request|push|schedule):') 'workflow must not have automatic triggers'
    Assert ($text -match 'github\.ref\s*==\s*[''\"]refs/heads/main[''\"]') 'workflow must guard main ref'
    Assert ($text -match '(?ms)^permissions:\s*\r?\n\s*contents:\s*read\s*$') 'workflow must grant only contents: read'
    $checkouts = [regex]::Matches($text, '(?ms)^\s*-\s*uses:\s*actions/checkout[^\r\n]*\r?\n(?<block>.*?)(?=^\s*-\s|\z)')
    Assert ($checkouts.Count -gt 0) 'workflow must use checkout'
    foreach ($checkout in $checkouts) {
        Assert ($checkout.Groups['block'].Value -match '(?m)^\s*with:\s*\r?\n(?:\s+.*\r?\n)*?\s*persist-credentials:\s*false\s*$') 'every actions/checkout block must set persist-credentials: false'
    }
    Assert ($text -notmatch '(?i)(GITHUB_TOKEN|GH_TOKEN|ACTIONS_ID_TOKEN_REQUEST_TOKEN|secrets\.|github\.token|credential(?:\.|\s)|http\.[^\s]*extraheader|https?://[^/\s]*@|git\s+config[^\r\n]*(?:credential|extraheader)|persist-credentials:\s*true)') 'workflow must not expose tokens, credentials, credential config, or credential URLs'
    Assert ($text -match 'CANDIDATE_SHA') 'workflow must assign candidate SHA to CANDIDATE_SHA'
    $trustedInputAssignment = '(?m)^\s*CANDIDATE_SHA:\s*\$\{\{\s*inputs\.candidate_sha\s*\}\}\s*$'
    $trustedAssignments = [regex]::Matches($text, $trustedInputAssignment)
    Assert ($trustedAssignments.Count -eq 1) 'workflow must contain exactly one trusted CANDIDATE_SHA input assignment'
    $withoutTrustedAssignment = [regex]::Replace($text, $trustedInputAssignment, '')
    Assert ($withoutTrustedAssignment -notmatch '(?i)(inputs\.candidate_sha|github\.event\.inputs\.candidate_sha|\$\{?CANDIDATE_SHA\}?|\$\{\{\s*env\.CANDIDATE_SHA)') 'workflow must not use alternate candidate SHA shell or ref flows'
    Assert ($text -match '\$env:CANDIDATE_SHA') 'workflow shell and ref use must flow through CANDIDATE_SHA environment only'
    Assert ($text -match '\^\[0-9a-f\]\{40\}\$') 'workflow must validate exact lowercase 40-hex SHA before use'
    Assert-EnvBoundOperation $text 'candidate fetch' '^.*git\s+fetch.*$'
    Assert-EnvBoundOperation $text 'detached candidate worktree' '^.*git\s+worktree\s+add\s+--detach.*$'
    Assert ($text -match 'git status --porcelain') 'workflow must require clean tracked trees'
    Assert-EnvBoundOperation $text 'trusted verifier invocation' '^.*verify-package-candidate\.ps1.*$'
    Assert-EnvBoundOperation $text 'candidate Cargo metadata invocation' '^.*cargo\s+metadata\s+--locked.*$'
    Assert-EnvBoundOperation $text 'candidate Cargo tree invocation' '^.*cargo\s+tree\s+--locked.*$'
    Assert ($text -match '(?is)(env\s+-i|Remove-Item\s+Env:).{0,2000}verify-package-candidate\.ps1') 'scrubbed candidate environment must bind verifier invocation'
    Assert ($text -match '(?is)(env\s+-i|Remove-Item\s+Env:).{0,2000}cargo\s+(?:metadata|tree)\s+--locked') 'scrubbed candidate environment must bind Cargo invocation'
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
