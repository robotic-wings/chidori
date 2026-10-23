[CmdletBinding()]
param([switch]$FixturesOnly)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:CanonicalCandidateWorktree = '$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA'

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
        Assert ($operation.Value -match '\$env:CANDIDATE_SHA') "$Name command/ref must contain literal `$env:CANDIDATE_SHA"
    }
}

function Assert-CanonicalWorktreeOperation([string]$Text, [string]$Name, [string]$Pattern) {
    $operations = [regex]::Matches($Text, $Pattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [Text.RegularExpressions.RegexOptions]::Multiline)
    Assert ($operations.Count -gt 0) "workflow must contain $Name"
    $canonicalPattern = [regex]::Escape($script:CanonicalCandidateWorktree) + '(?=$|[\s;"''])'
    foreach ($operation in $operations) {
        Assert ($operation.Value -match $canonicalPattern) "$Name must use canonical candidate worktree expression with a token/path boundary"
    }
}

function Assert-SafeCandidateExecution([string]$Text, [string]$Name, [string]$Pattern) {
    $lines = $Text -split "\r?\n"
    $operationCount = 0
    $canonicalPattern = [regex]::Escape($script:CanonicalCandidateWorktree) + '(?=$|[\s;"''])'
    foreach ($line in $lines) {
        # Comments cannot establish an execution context.  The operation must run
        # immediately inside one balanced canonical Push/Pop segment; a prior
        # location change (including one on this line) is not an execution context.
        $command = $line -replace '\s+#.*$', ''
        if ($command -match $Pattern) {
            $operationCount++
            $safeSegment = '^\s*Push-Location\s+(?:"|'')?' + $canonicalPattern + '(?:"|'')?\s*;\s*(?<operation>.*?)\s*;\s*Pop-Location\s*$'
            $match = [regex]::Match($command, $safeSegment, [Text.RegularExpressions.RegexOptions]::IgnoreCase)
            Assert $match.Success "$Name must execute immediately inside a balanced canonical Push-Location/Pop-Location segment"
            $operation = $match.Groups['operation'].Value
            Assert ($operation -match $Pattern) "$Name must be the operation inside the canonical execution segment"
            $locationMutation = '(?i)(?:^|[;|&]\s*)\s*(?:Set-Location|Push-Location|Pop-Location|cd|sl|chdir)\b'
            Assert ($operation -notmatch $locationMutation) "$Name execution segment must not change locations, including through PowerShell location aliases"
        }
    }
    Assert ($operationCount -gt 0) "workflow must contain $Name"
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
    $worktreeTarget = 'git\s+worktree\s+add\s+--detach\s+(?:"|'')?' + [regex]::Escape($script:CanonicalCandidateWorktree)
    Assert-CanonicalWorktreeOperation $text 'detached candidate worktree' "^.*$worktreeTarget.*$"
    Assert ($text -match 'git status --porcelain') 'workflow must require clean tracked trees'
    Assert-SafeCandidateExecution $text 'trusted verifier invocation' 'verify-package-candidate\.ps1'
    Assert-SafeCandidateExecution $text 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked'
    Assert-SafeCandidateExecution $text 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked'
    Assert ($text -match '(?m)&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File') 'trusted verifier must use the Ubuntu-compatible pwsh interpreter'
    Assert ($text -notmatch '(?im)&\s+powershell(?:\.exe)?\s+-NoProfile') 'trusted verifier must not use the Windows-only powershell interpreter'
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

function Invoke-StaticWorkflowFixtures {
    $happy = @'
git fetch origin $env:CANDIDATE_SHA
git worktree add --detach $env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA $env:CANDIDATE_SHA
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA; Pop-Location
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; cargo metadata --locked; Pop-Location
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; cargo tree --locked; Pop-Location
'@
    Assert-EnvBoundOperation $happy 'candidate fetch' '^.*git\s+fetch.*$'
    Assert-CanonicalWorktreeOperation $happy 'detached candidate worktree' '^.*git\s+worktree\s+add\s+--detach.*$'
    Assert-SafeCandidateExecution $happy 'trusted verifier invocation' 'verify-package-candidate\.ps1'
    Assert-SafeCandidateExecution $happy 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked'
    Assert-SafeCandidateExecution $happy 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked'

    $inertAlias = @'
$env:CANDIDATE_SHA
git fetch origin $candidate_alias
git worktree add --detach candidate $candidate_alias
powershell -File scripts/verify-package-candidate.ps1 -Candidate $candidate_alias
Set-Location "candidate-$candidate_alias"; cargo metadata --locked
Set-Location "candidate-$candidate_alias"; cargo tree --locked
'@
    Assert-Throws { Assert-EnvBoundOperation $inertAlias 'candidate fetch' '^.*git\s+fetch.*$' } 'inert earlier env reference must not authorize alias-backed fetch'
    Assert-Throws { Assert-CanonicalWorktreeOperation $inertAlias 'detached candidate worktree' '^.*git\s+worktree\s+add\s+--detach.*$' } 'inert earlier env reference must not authorize alias-backed worktree'
    Assert-Throws { Assert-SafeCandidateExecution $inertAlias 'trusted verifier invocation' 'verify-package-candidate\.ps1' } 'inert earlier env reference must not authorize alias-backed verifier'
    Assert-Throws { Assert-SafeCandidateExecution $inertAlias 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'inert earlier env reference must not authorize alias-backed metadata'
    Assert-Throws { Assert-SafeCandidateExecution $inertAlias 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'inert earlier env reference must not authorize alias-backed tree'

    $commentOnly = @'
cargo metadata --locked # Set-Location candidate-$env:CANDIDATE_SHA
cargo tree --locked # Set-Location candidate-$env:CANDIDATE_SHA
'@
    Assert-Throws { Assert-SafeCandidateExecution $commentOnly 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'comment-only candidate context must not authorize metadata'
    Assert-Throws { Assert-SafeCandidateExecution $commentOnly 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'comment-only candidate context must not authorize tree'

    $wrongWorktree = @'
Set-Location "trusted-$env:CANDIDATE_SHA"; cargo metadata --locked
Set-Location "trusted-$env:CANDIDATE_SHA"; cargo tree --locked
'@
    Assert-Throws { Assert-SafeCandidateExecution $wrongWorktree 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'wrong worktree must not authorize metadata'
    Assert-Throws { Assert-SafeCandidateExecution $wrongWorktree 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'wrong worktree must not authorize tree'

    $mismatchedWorktree = @'
git worktree add --detach $env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA $env:CANDIDATE_SHA
Set-Location "$env:RUNNER_TEMP/candidate-other-$env:CANDIDATE_SHA"; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA
Set-Location "$env:RUNNER_TEMP/candidate-other-$env:CANDIDATE_SHA"; cargo metadata --locked
Set-Location "$env:RUNNER_TEMP/candidate-other-$env:CANDIDATE_SHA"; cargo tree --locked
'@
    Assert-CanonicalWorktreeOperation $mismatchedWorktree 'detached candidate worktree' '^.*git\s+worktree\s+add\s+--detach.*$'
    Assert-Throws { Assert-SafeCandidateExecution $mismatchedWorktree 'trusted verifier invocation' 'verify-package-candidate\.ps1' } 'verifier must use worktree creation target exactly'
    Assert-Throws { Assert-SafeCandidateExecution $mismatchedWorktree 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'metadata must use worktree creation target exactly'
    Assert-Throws { Assert-SafeCandidateExecution $mismatchedWorktree 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'tree must use worktree creation target exactly'

    $suffixLookalike = @'
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA-other"; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA-other"; cargo metadata --locked
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA-other"; cargo tree --locked
'@
    Assert-Throws { Assert-SafeCandidateExecution $suffixLookalike 'trusted verifier invocation' 'verify-package-candidate\.ps1' } 'suffix lookalike must not authorize verifier worktree'
    Assert-Throws { Assert-SafeCandidateExecution $suffixLookalike 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'suffix lookalike must not authorize metadata worktree'
    Assert-Throws { Assert-SafeCandidateExecution $suffixLookalike 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'suffix lookalike must not authorize tree worktree'

    foreach ($suffix in @('/child', '/../trusted', '\child', '\..\trusted')) {
        $badWorktree = $script:CanonicalCandidateWorktree + $suffix
        $childOrTraversal = 'Set-Location "' + $badWorktree + '"; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA' + "`n" +
            'Set-Location "' + $badWorktree + '"; cargo metadata --locked' + "`n" +
            'Set-Location "' + $badWorktree + '"; cargo tree --locked'
        Assert-Throws { Assert-SafeCandidateExecution $childOrTraversal 'trusted verifier invocation' 'verify-package-candidate\.ps1' } "child/traversal suffix [$suffix] must not authorize verifier worktree"
        Assert-Throws { Assert-SafeCandidateExecution $childOrTraversal 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } "child/traversal suffix [$suffix] must not authorize metadata worktree"
        Assert-Throws { Assert-SafeCandidateExecution $childOrTraversal 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } "child/traversal suffix [$suffix] must not authorize tree worktree"
    }

    $canonicalThenTrusted = @'
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; Set-Location trusted; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; Set-Location trusted; cargo metadata --locked
Set-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; Set-Location trusted; cargo tree --locked
'@
    Assert-Throws { Assert-SafeCandidateExecution $canonicalThenTrusted 'trusted verifier invocation' 'verify-package-candidate\.ps1' } 'canonical then trusted location must not authorize verifier'
    Assert-Throws { Assert-SafeCandidateExecution $canonicalThenTrusted 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } 'canonical then trusted location must not authorize metadata'
    Assert-Throws { Assert-SafeCandidateExecution $canonicalThenTrusted 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } 'canonical then trusted location must not authorize tree'

    foreach ($alias in @('cd', 'CD', 'sl', 'SL', 'chdir', 'ChDiR')) {
        $aliasMutation = 'Push-Location "' + $script:CanonicalCandidateWorktree + '"; & ' + $alias + ' trusted; powershell -File scripts/verify-package-candidate.ps1 -Candidate $env:CANDIDATE_SHA; Pop-Location' + "`n" +
            'Push-Location "' + $script:CanonicalCandidateWorktree + '"; & ' + $alias + ' trusted; cargo metadata --locked; Pop-Location' + "`n" +
            'Push-Location "' + $script:CanonicalCandidateWorktree + '"; & ' + $alias + ' trusted; cargo tree --locked; Pop-Location'
        Assert-Throws { Assert-SafeCandidateExecution $aliasMutation 'trusted verifier invocation' 'verify-package-candidate\.ps1' } "$alias location alias must not authorize verifier"
        Assert-Throws { Assert-SafeCandidateExecution $aliasMutation 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked' } "$alias location alias must not authorize metadata"
        Assert-Throws { Assert-SafeCandidateExecution $aliasMutation 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked' } "$alias location alias must not authorize tree"
    }
}

Invoke-Fixtures
Invoke-StaticWorkflowFixtures
if (-not $FixturesOnly) {
    Assert-WorkflowContract (Join-Path $PSScriptRoot '..\.github\workflows\candidate-verifier.yml')
}
Write-Host 'PASS: candidate-verifier workflow contracts and hermetic fixtures'
