[CmdletBinding()]
param([switch]$FixturesOnly)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:CanonicalCandidateWorktree = '$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA'
$script:CanonicalTrustedRoot = '$env:GITHUB_WORKSPACE/trusted'
$script:RawBinaryFixturePending = $false
$script:RawBinaryFixturePendingReason = $null

function Assert([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    try { & $Action } catch { return }
    throw $Message
}

function Get-RawImmutableFixtureResult([bool]$ArgumentListAvailable) {
    if (-not $ArgumentListAvailable) {
        return [pscustomobject]@{ State = 'PENDING'; ExitCode = 2 }
    }
    return [pscustomobject]@{ State = 'READY'; ExitCode = 0 }
}

function Get-DefaultContractCapabilityResult([bool]$ArgumentListAvailable) {
    return [pscustomobject]@{
        ReportPending = -not $ArgumentListAvailable
        ContinueStaticContracts = $true
    }
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

function Assert-CanonicalWorktreeOperation([string]$Text, [string]$Name) {
    # The inline form is retained for the dedicated worktree-creation route.
    # A captured form is permitted only in the named materialization step, after
    # that same run block has captured the canonical SHA and worktree variables.
    $operations = [regex]::Matches($Text, '(?im)^\s*git\s+worktree\s+add\s+--detach\b.*$')
    Assert ($operations.Count -eq 1) "workflow must contain exactly one executable $Name operation"
    $operation = $operations[0]
    $inline = '^\s*git\s+worktree\s+add\s+--detach\s+(?:"|'')?' + [regex]::Escape($script:CanonicalCandidateWorktree) + '(?:"|'')?\s+\$env:CANDIDATE_SHA\s*$'
    if ($operation.Value -match $inline) { return }

    $capturedValid = $false
    $blocks = [regex]::Matches($Text, '(?ms)^[ \t]*-[ \t]*name:[ \t]*Validate identity and materialize candidate[ \t]*\r?\n.*?^[ \t]*run:[ \t]*\|[ \t]*\r?\n(?<run>.*?)(?=^[ \t]*-[ \t]+(?:name:|uses:)|\z)')
    foreach ($block in $blocks) {
        $run = $block.Groups['run'].Value
        $shaCapture = [regex]::Match($run, '(?im)^\s*\$candidateSha\s*=\s*\$env:CANDIDATE_SHA\s*$')
        $worktreeCapture = [regex]::Match($run, '(?im)^\s*\$candidateWorktree\s*=\s*Join-Path\s+\$env:RUNNER_TEMP\s+\(["'']candidate-\$candidateSha["'']\)\s*$')
        $capturedOperation = [regex]::Match($run, '(?im)^\s*git\s+worktree\s+add\s+--detach\s+\$candidateWorktree\s+\$candidateSha\s*$')
        $capturedAbsoluteIndex = $block.Groups['run'].Index + $capturedOperation.Index
        if ($shaCapture.Success -and $worktreeCapture.Success -and $capturedOperation.Success -and $capturedAbsoluteIndex -eq $operation.Index -and $shaCapture.Index -lt $capturedOperation.Index -and $worktreeCapture.Index -lt $capturedOperation.Index) {
            $capturedValid = $true
        }
    }
    Assert $capturedValid "$Name must use exact inline canonical form or the validated captured materialization form"
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

function Assert-NoCargoTargetDirFlags([string]$Text) {
    $tokens = $null
    $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseInput($Text, [ref]$tokens, [ref]$errors)
    Assert ($errors.Count -eq 0) 'Cargo target-dir inspection requires parseable PowerShell'
    $cargoCommands = $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] -and $node.GetCommandName() -ieq 'cargo' }, $true)
    foreach ($cargoCommand in $cargoCommands) {
        $elements = $cargoCommand.CommandElements
        if ($elements.Count -ge 2 -and $elements[1].Extent.Text -in @('metadata', 'tree')) {
            Assert (-not ($elements | Where-Object { $_.Extent.Text -match '(?i)^--target-dir(?:=|$)' })) 'literal Cargo metadata/tree commands must use CARGO_TARGET_DIR isolation, never a --target-dir CLI flag'
        }
    }
}

function Assert-FullUntrackedCleanGate([string]$Text, [string]$Variable, [string]$Name) {
    $escaped = [regex]::Escape($Variable)
    Assert ($Text -match ('(?is)' + $escaped + '\s*=\s*git\s+status\s+--porcelain\s+--untracked-files=all\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw.*?' + $escaped + '\s*-ne\s*[''\"]{2}.*?' + $escaped)) "$Name must capture full untracked porcelain, immediately check native exit, compare empty, and report its captured output"
}

function Assert-CleanMaterializationContract([string]$Text) {
    $blocks = [regex]::Matches($Text, '(?ms)^[ \t]*-[ \t]*name:[ \t]*Validate identity and materialize candidate[ \t]*\r?\n.*?^[ \t]*run:[ \t]*\|[ \t]*\r?\n(?<run>.*?)(?=^[ \t]*-[ \t]+(?:name:|uses:)|\z)')
    Assert ($blocks.Count -eq 1) 'workflow must define exactly one Validate identity and materialize candidate YAML run block'
    $run = $blocks[0].Groups['run'].Value
    $worktree = [regex]::Match($run, '(?im)^\s*git\s+worktree\s+add\s+--detach\s+\$candidateWorktree\s+\$candidateSha\s*$')
    Assert $worktree.Success 'materialization block must detach the canonical candidate worktree at the captured candidate SHA'
    $afterWorktree = $run.Substring($worktree.Index + $worktree.Length)
    $push = [regex]::Match($afterWorktree, '(?im)^\s*Push-Location\s+\$candidateWorktree\s*$')
    Assert $push.Success 'materialization block must enter the canonical candidate worktree before identity and clean evidence'
    $afterPush = $afterWorktree.Substring($push.Index + $push.Length)
    $head = [regex]::Match($afterPush, '(?im)^\s*\$materializedCandidateHead\s*=\s*git\s+rev-parse\s+HEAD\s*$\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw.*?\}\s*\r?\n\s*if\s*\(\s*\$materializedCandidateHead\s*-ne\s*\$candidateSha\s*\)\s*\{\s*throw.*?\}')
    Assert $head.Success 'materialization block must capture candidate HEAD, immediately check native exit, and compare it to the captured candidate SHA'
    $afterHead = $afterPush.Substring($head.Index + $head.Length)
    Assert-FullUntrackedCleanGate $afterHead '$materializedCandidatePorcelain' 'materialized candidate worktree clean gate'
    $gate = [regex]::Match($afterHead, '(?is)\$materializedCandidatePorcelain\s*=\s*git\s+status\s+--porcelain\s+--untracked-files=all.*?\$materializedCandidatePorcelain\s*-ne\s*[''\"]{2}.*?\$materializedCandidatePorcelain')
    $beforeGate = $afterHead.Substring(0, $gate.Index)
    Assert ($beforeGate -notmatch '(?i)(?:git\s+(?:show|diff)|\$candidateTarget\s*=|Set-Content|Add-Content|Out-File|Copy-Item|Move-Item|New-Item|CreateDirectory|Remove-Item|\[IO\.File\]::(?:Write|Create|Append)|cargo\s+|&\s+pwsh|verify-package-candidate)') 'no parent/path evidence or candidate-mutating setup, target creation, verifier, or Cargo work may precede materialized candidate clean proof'
}

function Assert-CandidateScrubAndCargoContract([string]$Text) {
    $lockedCargoBlock = [regex]::Matches($Text, '(?ms)^[ \t]*-[ \t]*name:[ \t]*Run locked Cargo checks in the candidate worktree[ \t]*\r?\n.*?^[ \t]*run:[ \t]*\|[ \t]*\r?\n(?<run>.*?)(?=^[ \t]*-[ \t]+(?:name:|uses:)|\z)')
    Assert ($lockedCargoBlock.Count -eq 1) 'workflow must define exactly one Run locked Cargo checks YAML run block'
    $Text = $lockedCargoBlock[0].Groups['run'].Value
    Assert ($Text -notmatch '(?im)^\s*#') 'comments cannot satisfy locked Cargo evidence requirements'
    Assert ($Text -notmatch '(?i)\b(?:Out-Null|Write-Host)\b') 'locked Cargo evidence must capture native output rather than discard it'
    $captureSha = '(?m)^\s*\$candidateSha\s*=\s*\$env:CANDIDATE_SHA\s*$'
    $captureWorktree = '(?m)^\s*\$candidateWorktree\s*=\s*Join-Path\s+\$env:RUNNER_TEMP\s+\(["'']candidate-\$candidateSha["'']\)\s*$'
    Assert ([regex]::Matches($Text, $captureSha).Count -eq 1) 'workflow must capture candidate SHA exactly once before environment scrub'
    Assert ([regex]::Matches($Text, $captureWorktree).Count -eq 1) 'workflow must capture canonical candidate worktree before environment scrub'
    $captureTarget = '(?m)^\s*\$candidateTarget\s*=\s*Join-Path\s+\$env:RUNNER_TEMP\s+[''"]candidate-target-\$candidateSha[''"]\s*$'
    Assert ([regex]::Matches($Text, $captureTarget).Count -eq 1) 'workflow must capture exactly one external candidate target before environment scrub'
    $captureEnd = [regex]::Match($Text, $captureWorktree).Index
    $firstScrub = [regex]::Match($Text, '(?im)^\s*Remove-Item\s+Env:\*').Index
    Assert ($firstScrub -ge 0 -and $captureEnd -lt $firstScrub) 'candidate SHA and canonical worktree capture must precede environment scrub'
    Assert ([regex]::Match($Text, $captureTarget).Index -lt $firstScrub) 'candidate target capture must precede environment scrub'
    Assert ($Text -match '(?im)^\s*\$candidateTargetCanonical\s*=\s*\[IO\.Path\]::GetFullPath\(\$candidateTarget\)') 'candidate target must be canonicalized before scrub'
    Assert ($Text -match '(?is)\$candidateTargetCanonical\s*-eq\s*\$candidateWorktreeCanonical') 'candidate target validation must reject the candidate worktree itself'

    $scrubbed = $Text.Substring($firstScrub)
    Assert ($scrubbed -notmatch '(?i)\$env:(?:RUNNER_TEMP|CANDIDATE_SHA|GITHUB_TOKEN|GH_TOKEN|ACTIONS_ID_TOKEN_REQUEST_TOKEN)') 'scrubbed execution must never re-read candidate/runtime or credential environment values'
    Assert ($scrubbed -notmatch '(?i)\$env:(?:GITHUB_WORKSPACE|GITHUB_SHA)\s*=') 'scrubbed execution must not restore GitHub identity environment values'
    foreach ($safe in @('PATH', 'HOME', 'CARGO_HOME', 'RUSTUP_HOME')) {
        Assert ($scrubbed -match ('(?im)^\s*\$env:' + $safe + '\s*=\s*\$')) "scrubbed execution must restore only the captured $safe environment value"
    }
    $cargoTargetRestore = '(?m)^\s*\$env:CARGO_TARGET_DIR\s*=\s*\$candidateTarget\s*$'
    Assert ([regex]::Matches($scrubbed, $cargoTargetRestore).Count -eq 1) 'scrubbed execution must restore exactly one captured candidate target directory'
    Assert ([regex]::Matches($scrubbed, '(?im)^\s*\$env:CARGO_TARGET_DIR\s*=').Count -eq 1) 'all scrubbed CARGO_TARGET_DIR assignments must be the one exact captured target restore'
    Assert ($scrubbed -notmatch '(?im)^\s*\$env:(?!(?:PATH|HOME|CARGO_HOME|RUSTUP_HOME|CARGO_TARGET_DIR)\b)[A-Za-z_][A-Za-z0-9_]*\s*=') 'scrubbed execution must not restore any environment variable outside PATH/HOME/CARGO_HOME/RUSTUP_HOME/CARGO_TARGET_DIR'

    $scope = '(?is)Push-Location\s+\$candidateWorktree\s*\r?\n\s*try\s*\{(?<body>.*?)cargo\s+metadata\s+--locked(?<afterMetadata>.*?)cargo\s+tree\s+--locked(?<afterTree>.*?)\}\s*finally\s*\{\s*Pop-Location\s*\}'
    $allCandidateScopes = [regex]::Matches($Text, '(?is)Push-Location\s+\$candidateWorktree\s*\r?\n\s*try\s*\{.*?\}\s*finally\s*\{\s*Pop-Location\s*\}')
    Assert ($allCandidateScopes.Count -eq 1) 'locked Cargo block must contain exactly one balanced canonical candidate Push-Location/try/finally/Pop-Location scope, including no-Cargo scopes'
    $scopeMatches = [regex]::Matches($Text, $scope)
    Assert ($scopeMatches.Count -eq 1) 'locked Cargo block must contain exactly one canonical candidate Push-Location/try/finally/Pop-Location scope'
    $match = $scopeMatches[0]
    Assert $match.Success 'metadata and tree must run in one balanced canonical candidate Push-Location/try/finally/Pop-Location scope'
    Assert ($match.Groups['body'].Value -notmatch '(?i)(?:Set-Location|Push-Location|Pop-Location|\bcd\b|\bsl\b|\bchdir\b)') 'candidate Cargo scope must not mutate the canonical location'
    Assert-FullUntrackedCleanGate $match.Value '$preTargetPorcelain' 'locked-Cargo pre-target clean gate'
    $preTargetGate = [regex]::Match($match.Value, '(?is)\$preTargetPorcelain\s*=\s*git\s+status\s+--porcelain\s+--untracked-files=all.*?\$preTargetPorcelain\s*-ne\s*[''\"]{2}.*?\$preTargetPorcelain')
    $beforePreTargetGate = $match.Value.Substring(0, $preTargetGate.Index)
    Assert ($beforePreTargetGate -notmatch '(?i)(?:\$candidateTarget\s*=|Set-Content|Add-Content|Out-File|Copy-Item|Move-Item|New-Item|CreateDirectory|Remove-Item|\[IO\.File\]::(?:Write|Create|Append)|cargo\s+|&\s+pwsh|verify-package-candidate)') 'no candidate-mutating setup, target creation, verifier, or Cargo work may precede locked-Cargo pre-target clean proof'
    $scopedVerifier = '&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\s+-Target\s+\$candidateTarget'
    $verifierInvocations = [regex]::Matches($Text, $scopedVerifier)
    Assert ($verifierInvocations.Count -eq 1) 'trusted verifier must run exactly once with the captured target'
    Assert ($verifierInvocations[0].Index -ge $match.Index -and ($verifierInvocations[0].Index + $verifierInvocations[0].Length) -le ($match.Index + $match.Length)) 'trusted verifier must run inside the sole canonical candidate scope'
    Assert ($match.Value -match ('(?is)Remove-Item\s+Env:\*.*?' + $cargoTargetRestore + '.*?' + $scopedVerifier + '.*?cargo\s+metadata\s+--locked')) 'trusted verifier must follow scrub and exact CARGO_TARGET_DIR restore before Cargo'
    $verifierPost = '\$verifierPostPorcelain\s*=\s*git\s+status\s+--porcelain\s+--untracked-files=all\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw.*?\$verifierPostPorcelain\s*-ne\s*[''\"]{2}.*?\$verifierPostPorcelain'
    Assert ($match.Value -match ('(?is)' + $scopedVerifier + '.*?' + $verifierPost + '.*?cargo\s+metadata\s+--locked')) 'verifier-post full porcelain evidence must follow verifier and precede Cargo'
    $literalMetadata = '(?im)^\s*cargo\s+metadata\s+--locked\b.*$'
    $literalTree = '(?im)^\s*cargo\s+tree\s+--locked\b.*$'
    Assert ([regex]::Matches($match.Value, $literalMetadata).Count -eq 1) 'candidate scope must use one literal cargo metadata --locked command'
    Assert ([regex]::Matches($match.Value, $literalTree).Count -eq 1) 'candidate scope must use one literal cargo tree --locked command'
    Assert-NoCargoTargetDirFlags $match.Value
    Assert ($match.Value -match ('(?is)' + $verifierPost + '.*?' + $literalMetadata + '\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw.*?' + $literalTree + '\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw')) 'verifier-post evidence and phase-local Git checks must precede literal Cargo commands with immediate exit throws'
    $preScope = '(?is)Push-Location\s+\$candidateWorktree\s*\r?\n\s*try\s*\{(?<pre>.*?)Remove-Item\s+Env:\*'
    $preMatch = [regex]::Match($Text, $preScope)
    Assert $preMatch.Success 'canonical candidate Push-Location/try scope must begin before pre-scrub evidence'
    foreach ($command in @('git\s+rev-parse\s+HEAD', 'git\s+status\s+--porcelain', 'git\s+show\s+-s\s+--format=%P\s+\$candidateSha', 'git\s+diff\s+--name-only')) {
        Assert ($preMatch.Groups['pre'].Value -match $command) "pre-scrub evidence [$command] must run inside the canonical candidate scope"
    }

    $evidenceCommands = @(
        'git\s+rev-parse\s+HEAD',
        'git\s+status\s+--porcelain',
        'git\s+show\s+-s\s+--format=%P\s+\$candidateSha',
        'git\s+diff\s+--name-only\s+9e1c295b1f3f90ef342021ec81b52ece7363cd6c\s+\$candidateSha',
        'cargo\s+metadata\s+--locked',
        'cargo\s+tree\s+--locked'
    )
    foreach ($command in $evidenceCommands) {
        $commands = [regex]::Matches($Text, '(?im)^\s*(?:\$[A-Za-z_][A-Za-z0-9_]*\s*=\s*\(?)?' + $command + '.*$')
        Assert ($commands.Count -gt 0) "workflow must contain evidence command [$command]"
        foreach ($native in $commands) {
            Assert ($native.Index -ge $match.Index -and ($native.Index + $native.Length) -le ($match.Index + $match.Length)) "candidate evidence command [$command] must not occur outside the sole canonical candidate scope"
            $following = $Text.Substring($native.Index + $native.Length, [Math]::Min(400, $Text.Length - $native.Index - $native.Length))
            Assert ($following -match '(?is)^\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw\b') "native evidence command [$command] must have an immediately adjacent LASTEXITCODE throw"
        }
    }

    Assert ($Text -match '(?im)^\s*\$candidateHead\s*=\s*git\s+rev-parse\s+HEAD') 'locked Cargo block must capture candidate HEAD output'
    Assert ($Text -match '(?im)^\s*\$candidateStatus\s*=\s*git\s+status\s+--porcelain') 'locked Cargo block must capture candidate status output'
    Assert ($Text -match '(?im)^\s*\$parents\s*=\s*\(git\s+show\s+-s\s+--format=%P\s+\$candidateSha\)') 'locked Cargo block must capture candidate parent output'
    Assert ($Text -match '(?im)^\s*\$candidatePaths\s*=\s*git\s+diff\s+--name-only\s+9e1c295b1f3f90ef342021ec81b52ece7363cd6c\s+\$candidateSha') 'locked Cargo block must capture base-to-candidate path scope using captured candidate SHA'
    Assert ($Text -match '(?is)\$candidateHead\s*-ne\s*\$candidateSha') 'locked Cargo block must compare captured HEAD to captured candidate SHA'
    Assert ($Text -match '(?is)\$candidateStatus\s*-ne\s*[''"]{2}') 'locked Cargo block must compare captured status for cleanliness'
    Assert ($Text -match '(?is)\$parents\.Count\s*-ne\s*1.{0,500}\$parents\[0\]\s*-ne\s*[''"]9e1c295b1f3f90ef342021ec81b52ece7363cd6c') 'locked Cargo block must compare captured sole parent to approved base'
    Assert ([regex]::Matches($Text, '(?im)^\s*\$candidatePathText\s*=\s*\(\$candidatePaths\s+-join\s+["'']`n["'']\)\s*$').Count -ge 3) 'locked Cargo block must normalize each captured path array to an ordered scalar'
    Assert ([regex]::Matches($Text, '(?is)\$candidatePathText\s*-ne\s*\$approvedCandidatePaths').Count -ge 3) 'locked Cargo block must compare each normalized path scalar to approved paths'
    Assert ($Text -notmatch '(?is)\$candidatePaths\s*-ne\s*\$approvedCandidatePaths') 'locked Cargo block must not compare a raw path array to an approved scalar'
    Assert ($Text -notmatch '(?i)\$candidatePaths.{0,100}(?:Sort-Object|\|\s*sort)') 'locked Cargo block must preserve captured path ordering without sorting'

    $phaseCommands = @('git\s+rev-parse\s+HEAD', 'git\s+status\s+--porcelain', 'git\s+show\s+-s\s+--format=%P\s+\$candidateSha', 'git\s+diff\s+--name-only\s+9e1c295b1f3f90ef342021ec81b52ece7363cd6c\s+\$candidateSha')
    foreach ($phase in @(
        @{ Name = 'pre-scrub'; Prefix = '(?is)\$candidateWorktree\s*=.*?'; Suffix = 'Remove-Item\s+Env:\*' },
        @{ Name = 'post-scrub/pre-Cargo'; Prefix = '(?is)\$verifierPostPorcelain.*?'; Suffix = 'cargo\s+metadata\s+--locked' },
        @{ Name = 'post-Cargo'; Prefix = '(?is)cargo\s+tree\s+--locked.*?'; Suffix = '\}\s*finally\s*\{\s*Pop-Location' }
    )) {
        foreach ($command in $phaseCommands) {
            $bound = $phase.Prefix + '(?:\$[A-Za-z_][A-Za-z0-9_]*\s*=\s*\(?)?' + $command + '.*?\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw.*?' + $phase.Suffix
            Assert ($Text -match $bound) "$($phase.Name) phase must bind [$command] with an immediately adjacent LASTEXITCODE throw"
        }
        $parentBound = $phase.Prefix + '\$parents\.Count.*?\$parents\[0\].*?' + $phase.Suffix
        Assert ($Text -match $parentBound) "$($phase.Name) phase must retain sole-parent/base evidence"
        foreach ($comparison in @(
            '\$candidateHead\s*-ne\s*\$candidateSha',
            '\$candidateStatus\s*-ne\s*[''"]{2}',
            '\$parents\.Count\s*-ne\s*1',
            '\$parents\[0\]\s*-ne\s*[''"]9e1c295b1f3f90ef342021ec81b52ece7363cd6c',
            '\$candidatePathText\s*-ne\s*\$approvedCandidatePaths'
        )) {
            Assert ($Text -match ($phase.Prefix + $comparison + '.*?' + $phase.Suffix)) "$($phase.Name) phase must compare captured evidence [$comparison] inside its own boundary"
        }
    }
}

function Assert-ExternalTargetContract([string]$Text) {
    $lockedCargoBlock = [regex]::Matches($Text, '(?ms)^[ \t]*-[ \t]*name:[ \t]*Run locked Cargo checks in the candidate worktree[ \t]*\r?\n.*?^[ \t]*run:[ \t]*\|[ \t]*\r?\n(?<run>.*?)(?=^[ \t]*-[ \t]+(?:name:|uses:)|\z)')
    Assert ($lockedCargoBlock.Count -eq 1) 'external candidate target identity must be bound inside exactly one locked Cargo YAML run block'
    $Text = $lockedCargoBlock[0].Groups['run'].Value
    $scrub = [regex]::Match($Text, '(?im)^\s*Remove-Item\s+Env:\*')
    Assert $scrub.Success 'external candidate target must be captured before the relevant environment scrub'
    Assert ($Text -match '(?im)^\s*\$candidateTarget\s*=\s*Join-Path\s+\$env:RUNNER_TEMP\s+[''"]candidate-target-\$candidateSha[''"]\s*$') 'workflow must capture exactly one external candidate target before scrub'
    Assert ([regex]::Matches($Text, '(?im)^\s*\$candidateTarget\s*=').Count -eq 1) 'workflow must capture candidate target exactly once'
    Assert ([regex]::Match($Text, '(?im)^\s*\$candidateTarget\s*=').Index -lt $scrub.Index) 'candidate target capture must precede the relevant environment scrub in the same run block'
    Assert ($Text -notmatch '(?im)^\s*\$candidateTarget\s*=.*\$candidateWorktree') 'candidate target must be strictly external to canonical worktree'
    Assert ($Text -notmatch '(?i)candidate-target-\$candidateSha(?:[/\\]|-|\.\.)') 'candidate target must reject child, suffix, or traversal forms'
    Assert ($Text -match '(?is)(?:GetFullPath|Resolve-Path).*\$candidateTarget') 'candidate target must be canonicalized before use'
    Assert ($Text -match '(?is)DirectorySeparatorChar.*?AltDirectorySeparatorChar') 'candidate target boundary must be separator-aware'
    Assert ($Text -match '(?is)\$candidateTargetCanonical\s*-eq\s*\$candidateWorktreeCanonical') 'candidate target boundary must reject equality with candidate worktree'
    Assert ($Text -match '(?is)\$candidateTargetCanonical.*?(?:StartsWith|like).*?(?:\$separator|\$alternateSeparator|DirectorySeparatorChar|AltDirectorySeparatorChar)') 'candidate target boundary must reject only separator-bound descendants while allowing siblings'
    Assert ($Text -notmatch '(?i)git\s+clean') 'cleanup must not broadly clean candidate state'
    Assert ($Text -match '(?is)Remove-Item\s+-LiteralPath\s+\$candidateTarget.{0,500}Test-Path\s+-LiteralPath\s+\$candidateTarget') 'external target cleanup must be narrow and prove post-clean absence'
    Assert ($Text -match '(?is)Remove-Item\s+Env:\*.*?&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\s+-Target\s+\$candidateTarget') 'scrubbed verifier must receive exactly captured external target through the trusted pwsh invocation'
    Assert ($Text -match '(?is)Remove-Item\s+Env:\*.*?\$env:CARGO_TARGET_DIR\s*=\s*\$candidateTarget.*?cargo\s+metadata\s+--locked') 'scrubbed metadata must receive the captured external target through CARGO_TARGET_DIR'
    Assert ($Text -match '(?is)Remove-Item\s+Env:\*.*?\$env:CARGO_TARGET_DIR\s*=\s*\$candidateTarget.*?cargo\s+tree\s+--locked') 'scrubbed tree must receive the captured external target through CARGO_TARGET_DIR'
    Assert ($Text -match '(?im)^\s*&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\s+-Target\s+\$candidateTarget\s*$') 'trusted verifier must receive the exact captured target value through the required pwsh invocation'
    Assert ($Text -match '(?im)^\s*cargo\s+metadata\s+--locked\b.*$') 'metadata must be a literal locked Cargo command using the restored environment target'
    Assert ($Text -match '(?im)^\s*cargo\s+tree\s+--locked\b.*$') 'tree must be a literal locked Cargo command using the restored environment target'
    Assert ($Text -match '(?is)Remove-Item\s+Env:\*.*?\$env:CARGO_TARGET_DIR\s*=\s*\$candidateTarget') 'scrubbed execution must restore captured CARGO_TARGET_DIR'
    Assert ($Text -match '(?is)(?:&\s+pwsh|cargo\s+tree).*?Remove-Item\s+-LiteralPath\s+\$candidateTarget') 'external target cleanup must follow verifier and Cargo use'
    foreach ($phase in @('verifierPre', 'verifierPost', 'outerPre', 'outerPostScrub', 'outerPostCargo', 'finalPostTargetCleanup')) {
        $variable = '$' + $phase + 'Porcelain'
        Assert ($Text -match ('(?is)' + [regex]::Escape($variable) + '\s*=\s*git\s+status\s+--porcelain\s+--untracked-files=all\s*\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\).*?' + [regex]::Escape($variable) + '\s*-ne\s*[''\"]{2}.*?' + [regex]::Escape($variable))) "named full porcelain block [$phase] must capture, immediately exit-check, compare, and report its own output"
    }
    Assert ($Text -match '(?is)\$verifierPrePorcelain.*?Remove-Item\s+Env:\*.*?&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\s+-Target\s+\$candidateTarget.*?\$verifierPostPorcelain.*?\$outerPrePorcelain.*?\$outerPostScrubPorcelain.*?cargo\s+metadata.*?cargo\s+tree.*?\$outerPostCargoPorcelain.*?Remove-Item\s+-LiteralPath\s+\$candidateTarget\s+-Recurse\s+-Force.*?\$finalPostTargetCleanupPorcelain') 'named porcelain phase blocks must remain ordered around verifier, scrub, Cargo, and cleanup'
    Assert ($Text -match '(?is)(?:New-Item|CreateDirectory).*?\$candidateTarget.*?(?:&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass|cargo\s+metadata)') 'verified external target directory must be created before use'
    Assert ($Text -match '(?is)(?:&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass|cargo\s+tree).*?Remove-Item\s+-LiteralPath\s+\$candidateTarget\s+-Recurse\s+-Force.*?Test-Path\s+-LiteralPath\s+\$candidateTarget.*?\$finalPostTargetCleanupPorcelain') 'nonempty external target cleanup must be narrow, post-use, proved absent, and followed by final clean evidence'
    Assert ($Text -notmatch '(?i)(?:Remove-Item\s+-LiteralPath\s+(?:\*|\$candidateWorktree)|git\s+clean)') 'cleanup must not use wildcard, candidate-root deletion, or git clean'
    foreach ($operation in @('&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\s+-Target\s+\$candidateTarget', 'cargo\s+metadata\s+--locked\b', 'cargo\s+tree\s+--locked\b')) {
        Assert ($Text -match ('(?is)' + $operation + '.*?\r?\n\s*if\s*\(\s*\$LASTEXITCODE\s*-ne\s*0\s*\)\s*\{\s*throw')) "external target operation [$operation] requires an immediate native exit guard"
    }
}

function Assert-TrustedVerifierIdentity([string]$Text) {
    $trustedCheckout = '(?ms)^\s*-\s*uses:\s*actions/checkout[^\r\n]*\r?\n(?<block>.*?)(?=^\s*-\s|\z)'
    $checkout = [regex]::Match($Text, $trustedCheckout)
    Assert $checkout.Success 'workflow must check out the trusted verifier'
    Assert ($checkout.Groups['block'].Value -match '(?m)^\s*path:\s*trusted\s*$') 'trusted checkout must use path trusted'

    $rootAssignment = '(?m)^\s*\$trustedRoot\s*=\s*Join-Path\s+\$env:GITHUB_WORKSPACE\s+[''\"]trusted[''\"]\s*$'
    Assert ([regex]::Matches($Text, $rootAssignment).Count -eq 1) 'trusted root must be derived exactly once from GITHUB_WORKSPACE and checkout path trusted'
    $verifierAssignment = '(?m)^\s*\$trustedVerifier\s*=\s*Join-Path\s+\$trustedRoot\s+[''\"]scripts[/\\]verify-package-candidate\.ps1[''\"]\s*$'
    Assert ([regex]::Matches($Text, $verifierAssignment).Count -eq 1) 'trusted verifier must be derived from the canonical trusted root'
    Assert ($Text -notmatch '(?im)^\s*\$trusted(?:Root|Verifier)\s*=.*\$env:RUNNER_TEMP') 'trusted verifier must not be rooted in RUNNER_TEMP'

    $failClosedGuard = '(?is)if\s*\(\s*-not\s*\(\s*Test-Path\s+-LiteralPath\s+\$trustedVerifier\s*\)\s*\)\s*\{\s*(?:throw\b|exit\s+(?:-[1-9]\d*|[1-9]\d*)\b)'
    Assert ([regex]::Matches($Text, $failClosedGuard).Count -eq 1) 'trusted verifier must have exactly one fail-closed Test-Path guard through its canonical variable'
    $invocation = '(?im)&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\b'
    Assert ([regex]::Matches($Text, $invocation).Count -eq 1) 'trusted verifier must be invoked exactly once through the canonical variable with pwsh -File'
    Assert ($Text -notmatch '(?im)&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+(?!\$trustedVerifier\b)') 'trusted verifier invocation must not bypass the canonical variable'

    $ordered = $failClosedGuard + '(?s:.{0,4000}?)Remove-Item\s+Env:.*?' + [regex]::Escape('& pwsh -NoProfile -ExecutionPolicy Bypass -File $trustedVerifier')
    Assert ($Text -match $ordered) 'trusted verifier fail-closed guard must precede environment scrub and invocation'
}

function Assert-VerifierScriptPortability([string]$Text) {
    # Only executable text is relevant: comments must not make a portable script
    # fail, and must not be used to satisfy the bare-git requirement.
    $commands = foreach ($line in ($Text -split "\r?\n")) {
        $withoutWholeLineComment = $line -replace '^\s*#.*$', ''
        $withoutWholeLineComment -replace '\s+#.*$', ''
    }
    $executableText = $commands -join "`n"
    $gitExe = '(?im)(?:^|[;|&]\s*)&?\s*(?:[''\"])?git\.exe(?:[''\"])?(?=$|[\s;|&])'
    $cmd = '(?im)(?:^|[;|&]\s*)&?\s*(?:[''\"])?cmd(?:\.exe)?(?:[''\"])?(?=$|[\s;|&])'
    Assert ($executableText -notmatch $gitExe) 'trusted verifier must not invoke git.exe, including quoted, mixed-case, or ampersand forms'
    Assert ($executableText -notmatch $cmd) 'trusted verifier must not invoke cmd or cmd.exe, including quoted, mixed-case, or ampersand forms'
}

function Assert-VerifierScriptRecursionSafety([string]$Text) {
    $lookupPattern = 'Get-Command\s+git\s+-All\s+-CommandType\s+Application\b'
    Assert ([regex]::Matches($Text, $lookupPattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase).Count -eq 1) 'trusted verifier must resolve ordered PATH Application candidates for git exactly once'
    Assert ($Text -match '(?im)^\s*function\s+Resolve-GitExecutablePath\b') 'trusted verifier must define a deterministic git executable-path resolver'
    Assert ($Text -match '(?is)foreach\s*\([^\)]*Get-Command\s+git\s+-All\s+-CommandType\s+Application') 'trusted verifier resolver must inspect Application candidates in command/PATH order'
    Assert ($Text -match '(?is)(?:\.Source|\.Path).{0,500}Test-Path\s+-LiteralPath\s+\$raw\s+-PathType\s+Leaf.{0,500}(?:GetFullPath|Resolve-Path)') 'trusted verifier resolver must validate each candidate-derived Source/Path as an existing leaf before canonicalization'
    Assert ($Text -match '(?is)(HashSet|Distinct)') 'trusted verifier resolver must de-duplicate equivalent candidates while preserving order'
    Assert ($Text -match '(?is)Resolve-GitExecutablePath.{0,3000}throw') 'trusted verifier resolver must fail closed when no valid executable candidate exists'
    Assert ($Text -notmatch '(?im)^\s*\$[A-Za-z_][A-Za-z0-9_]*\s*=\s*\(\s*Get-Command\s+git[^\r\n]*\)\.(?:Source|Path)') 'trusted verifier must not coerce a Get-Command collection Source/Path directly to a scalar'

    $resolutionPattern = '(?im)^\s*(?<variable>\$[A-Za-z_][A-Za-z0-9_]*)\s*=\s*Resolve-GitExecutablePath\s*$'
    $resolutions = [regex]::Matches($Text, $resolutionPattern)
    Assert ($resolutions.Count -eq 1) 'trusted verifier must select exactly one scalar executable path from the deterministic resolver'
    Assert ($Text -notmatch '(?im)^\s*function\s+git\b') 'trusted verifier must not declare a Git helper or function that can recurse'

    $resolved = [regex]::Escape($resolutions[0].Groups['variable'].Value)
    Assert ($Text -match ('(?im)&\s*' + $resolved + '\b')) 'trusted verifier wrapper invocations must use the resolved git executable-path variable'
    Assert ($Text -notmatch '(?im)(?:&\s*|Start-Process\s+-FilePath\s+)\$[A-Za-z_][A-Za-z0-9_]*\.(?:Source|Path)\b') 'trusted verifier must not invoke raw ApplicationInfo Source/Path members'

    $invocations = [regex]::Matches($Text, '(?im)&\s*(?<variable>\$[A-Za-z_][A-Za-z0-9_]*)\b')
    foreach ($invocation in $invocations) {
        Assert ($invocation.Groups['variable'].Value -ceq $resolutions[0].Groups['variable'].Value) 'trusted verifier wrapper invocation must not use a git alias or raw ApplicationInfo variable'
    }
    $startProcesses = [regex]::Matches($Text, '(?im)Start-Process\s+-FilePath\s+(?<variable>\$[A-Za-z_][A-Za-z0-9_]*)\b')
    foreach ($startProcess in $startProcesses) {
        Assert ($startProcess.Groups['variable'].Value -ceq $resolutions[0].Groups['variable'].Value) 'trusted verifier Start-Process must not use a git alias or raw ApplicationInfo variable'
    }

    $commands = foreach ($line in ($Text -split "\r?\n")) {
        $withoutWholeLineComment = $line -replace '^\s*#.*$', ''
        $withoutWholeLineComment -replace '\s+#.*$', ''
    }
    $executableText = $commands -join "`n"
    Assert ($executableText -notmatch '(?im)(?:^|[;|&]\s*)&?\s*(?:[''\"])?git(?:\.exe)?(?:[''\"])?(?=$|[\s;|&])') 'trusted verifier must not bypass the resolved git Application variable with a direct git invocation'
    Assert ($executableText -notmatch '(?im)Start-Process\s+-FilePath\s+(?:[''\"])?git(?:\.exe)?(?:[''\"])?(?=$|[\s;|&])') 'trusted verifier Start-Process must not bypass the resolved git Application variable'
}

function Get-ContainingFunctionName([System.Management.Automation.Language.Ast]$Node) {
    for ($parent = $Node.Parent; $null -ne $parent; $parent = $parent.Parent) {
        if ($parent -is [System.Management.Automation.Language.FunctionDefinitionAst]) { return $parent.Name }
    }
    return $null
}

function Get-BoundCommandArgument([System.Management.Automation.Language.CommandAst]$Command, [string]$ParameterName, [int]$FallbackIndex) {
    $elements = $Command.CommandElements
    for ($index = 0; $index -lt $elements.Count; $index++) {
        if ($elements[$index] -is [System.Management.Automation.Language.CommandParameterAst] -and $elements[$index].ParameterName -ieq $ParameterName) {
            if ($index + 1 -lt $elements.Count -and -not ($elements[$index + 1] -is [System.Management.Automation.Language.CommandParameterAst])) { return $elements[$index + 1] }
            return $null
        }
    }
    if ($elements.Count -gt $FallbackIndex) { return $elements[$FallbackIndex] }
    return $null
}

function Get-ArgumentVectorElements([System.Management.Automation.Language.Ast]$Argument) {
    if ($Argument -isnot [System.Management.Automation.Language.ArrayExpressionAst]) { return $null }
    $statements = $Argument.SubExpression.Statements
    if ($statements.Count -ne 1 -or $statements[0] -isnot [System.Management.Automation.Language.PipelineAst]) { return $null }
    $pipeline = $statements[0].PipelineElements
    if ($pipeline.Count -ne 1 -or $pipeline[0] -isnot [System.Management.Automation.Language.CommandExpressionAst]) { return $null }
    $expression = $pipeline[0].Expression
    if ($expression -isnot [System.Management.Automation.Language.ArrayLiteralAst]) { return $null }
    return ,@($expression.Elements)
}

function Assert-NonReplayLiteralVector([System.Management.Automation.Language.Ast]$Argument, [string]$Route) {
    $elements = Get-ArgumentVectorElements $Argument
    Assert ($null -ne $elements -and $elements.Count -gt 0 -and $elements[0] -is [System.Management.Automation.Language.StringConstantExpressionAst]) "$Route must use a literal, unsplatted command argument vector"
    $verb = $elements[0].Value.ToLowerInvariant()
    Assert ($verb -notin @('apply', 'patch', 'am')) 'trusted verifier must not replay lock changes through apply, patch, am, or diff --binary'
    if ($verb -eq 'diff') {
        Assert (-not (@($elements | Where-Object { $_ -is [System.Management.Automation.Language.StringConstantExpressionAst] } | ForEach-Object Value) -contains '--binary')) 'trusted verifier must not replay lock changes through apply, patch, am, or diff --binary'
    }
}

function Assert-NoPatchReplay([string]$Text) {
    $tokens = $null
    $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseInput($Text, [ref]$tokens, [ref]$errors)
    Assert ($errors.Count -eq 0) 'trusted verifier replay analysis requires parseable PowerShell'
    $commands = $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] }, $true)
    foreach ($command in $commands) {
        $name = $command.GetCommandName()
        $elements = $command.CommandElements
        $functionName = Get-ContainingFunctionName $command
        $isResolvedDirect = $command.InvocationOperator -eq [System.Management.Automation.Language.TokenKind]::Ampersand -and $elements.Count -gt 0 -and $elements[0] -is [System.Management.Automation.Language.VariableExpressionAst]
        if ($isResolvedDirect) {
            $argument = if ($elements.Count -gt 1) { $elements[1] } else { $null }
            if ($functionName -eq 'Invoke-ResolvedGit' -and $argument -is [System.Management.Automation.Language.VariableExpressionAst]) { continue }
            if ($argument -is [System.Management.Automation.Language.VariableExpressionAst]) { throw 'resolved direct Git route must not use a variable or splatted argument vector' }
            if ($argument -is [System.Management.Automation.Language.StringConstantExpressionAst]) {
                $verb = $argument.Value.ToLowerInvariant()
                Assert ($verb -notin @('apply', 'patch', 'am', 'diff')) 'trusted verifier must not replay lock changes through apply, patch, am, or diff --binary'
            } else { throw 'resolved direct Git route must use a literal command token' }
            continue
        }

        if ($name -eq 'Invoke-ResolvedGit') {
            Assert-NonReplayLiteralVector (Get-BoundCommandArgument $command 'Arguments' 1) 'resolved Git wrapper'
            continue
        }
        if ($name -eq 'Write-GitOutput') {
            Assert-NonReplayLiteralVector (Get-BoundCommandArgument $command 'Arguments' 2) 'resolved Git output wrapper'
            continue
        }
        if ($name -eq 'Start-Process') {
            if (@($elements | Where-Object { $_ -is [System.Management.Automation.Language.VariableExpressionAst] -and $_.Splatted }).Count -gt 0) {
                throw 'resolved Start-Process Git route must not use splatted parameters'
            }
            $filePath = Get-BoundCommandArgument $command 'FilePath' 2
            if ($filePath -is [System.Management.Automation.Language.VariableExpressionAst]) {
                $argument = Get-BoundCommandArgument $command 'ArgumentList' 4
                if ($functionName -eq 'Write-GitOutput' -and $argument -is [System.Management.Automation.Language.VariableExpressionAst]) { continue }
                Assert-NonReplayLiteralVector $argument 'resolved Start-Process Git route'
            }
        }
    }
}

function Assert-RawBinaryCaptureContract([string]$Text) {
    $helper = '(?im)^\s*function\s+Write-RawGitOutput\b'
    Assert ([regex]::Matches($Text, $helper).Count -eq 1) 'trusted verifier must define exactly one raw binary Git capture helper'
    Assert ($Text -match '(?is)function\s+Write-RawGitOutput.{0,2000}(?:FileName|FilePath)\s*=\s*\$gitExecutablePath') 'raw capture helper must use the resolved Git executable path'
    Assert ($Text -match '(?is)ProcessStartInfo.{0,2500}UseShellExecute\s*=\s*\$false.{0,2500}RedirectStandardOutput\s*=\s*\$true') 'raw capture helper must configure a non-shell ProcessStartInfo stdout stream'
    Assert ($Text -match '(?is)function\s+Write-RawGitOutput.{0,2500}ArgumentList\.Add\(') 'raw capture helper must add each Git argument through ProcessStartInfo.ArgumentList'
    Assert ($Text -notmatch '(?is)function\s+Write-RawGitOutput.{0,2500}\.Arguments\s*=') 'raw capture helper must not construct a text Arguments command line'
    Assert ($Text -match '(?is)FileStream.{0,2000}StandardOutput\.BaseStream\.CopyTo\(.{0,1500}Flush') 'raw capture helper must copy StandardOutput.BaseStream directly into a FileStream and flush it'
    Assert ($Text -match '(?is)WaitForExit.{0,1000}ExitCode.{0,1000}(?:throw|if)') 'raw capture helper must wait for and fail closed on Git exit status'
    Assert ($Text -match '(?is)(?:Close|Dispose).{0,1500}(?:Close|Dispose)') 'raw capture helper must close process and file resources'
    Assert ($Text -notmatch '(?is)Start-Process\b|RedirectStandardOutput\s+\$|\|\s*(?:Out-String|Set-Content|Add-Content)|\b(?:Out-String|Set-Content|Add-Content)\b|>\s*\$|-[Ee]ncoding') 'trusted verifier must not use text, Start-Process, shell-redirection, or encoding-normalized Git capture'
    foreach ($role in @('packageLock', 'baseLock', 'ctutilsLock', 'candidateLock', 'mergedLock')) {
        Assert ($Text -match ('(?is)Write-RawGitOutput\s+\$' + $role + '\b')) "trusted verifier must bind raw capture helper to fixed $role output"
    }
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$packageLock.{0,700}@\([''\"]show[''\"].{0,700}\$packageCommit.*Cargo\.lock') 'raw capture must use the fixed package show snapshot'
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$baseLock.{0,700}@\([''\"]show[''\"].{0,700}\$base.*Cargo\.lock') 'raw capture must use the fixed base show snapshot'
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$ctutilsLock.{0,700}@\([''\"]show[''\"].{0,700}\$ctutilsCommit.*Cargo\.lock') 'raw capture must use the fixed ctutils show snapshot'
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$candidateLock.{0,700}@\([''\"]show[''\"].{0,700}\$candidateSha.*Cargo\.lock') 'raw capture must use the fixed candidate lock snapshot'
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$mergedLock.{0,900}@\([''\"]merge-file[''\"].{0,700}[''\"]-p[''\"].{0,700}\$packageLock.{0,700}\$baseLock.{0,700}\$ctutilsLock') 'raw capture must use fixed package/base/ctutils merge-file -p ordering'
    $sourceHashes = @{
        packageLock = '9e36a2e89363eeedc4215a68dbd51f33dfec3741'
        baseLock = '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9'
        ctutilsLock = 'fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66'
        candidateLock = 'c4b4316368317a342058f77151ab8043afea676b'
    }
    foreach ($role in $sourceHashes.Keys) {
        $hash = $sourceHashes[$role]
        Assert ($Text -match ("(?is)Write-RawGitOutput\s+\$" + $role + "\b.{0,1400}hash-object.{0,700}\$" + $role + ".{0,700}" + $hash)) "raw $role capture must be source-byte hash-bound before composition"
    }
    Assert ($Text -match '(?is)Write-RawGitOutput\s+\$mergedLock.{0,1400}hash-object.{0,700}\$mergedLock.{0,700}c4b4316368317a342058f77151ab8043afea676b.{0,700}(?:ReadAllBytes|candidateLock|Blob\s+\$candidateSha)') 'raw emitted merge bytes must be hash-bound before candidate byte comparison'
    Assert ($Text -notmatch '(?is)\$[A-Za-z_][A-Za-z0-9_]*\s*=\s*@\([''\"](?:show|merge-file)[''\"].{0,1000}Write-RawGitOutput') 'raw capture must not choose composition roles/options dynamically'
}

function Assert-LockCompositionContract([string]$Text) {
    foreach ($required in @(
        '59ebb109', '00148aa', '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9',
        '9e36a2e89363eeedc4215a68dbd51f33dfec3741', 'fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66',
        'c4b4316368317a342058f77151ab8043afea676b'
    )) { Assert ($Text -match [regex]::Escape($required)) "trusted verifier must bind lock composition provenance [$required]" }
    Assert ($Text -match '(?is)\$packageCommit\s*=\s*[''\"]59ebb109[''\"]') 'trusted verifier must name the package direct commit'
    Assert ($Text -match '(?is)\$ctutilsCommit\s*=\s*[''\"]00148aa[''\"]') 'trusted verifier must name the ctutils direct commit'
    Assert ($Text -match '(?is)show.{0,300}--name-only.{0,500}\$packageCommit') 'trusted verifier must inspect package direct changed paths'
    Assert ($Text -match '(?is)show.{0,300}--name-only.{0,500}\$ctutilsCommit') 'trusted verifier must inspect ctutils direct changed paths'
    Assert ($Text -match '(?is)Assert-Equal\s+\$directPaths') 'trusted verifier must require the exact package changed-path set'
    Assert ($Text -match '(?is)Assert-Equal\s+\$ctutilsPaths') 'trusted verifier must require the exact ctutils changed-path set'
    Assert ($Text -match '(?is)\$packageCommit\^.{0,500}47fbe5113e4b7c18cf2cc96f3a0410f045a281a9') 'trusted verifier must bind package parent lock base'
    Assert ($Text -match '(?is)\$ctutilsCommit\^.{0,500}47fbe5113e4b7c18cf2cc96f3a0410f045a281a9') 'trusted verifier must bind ctutils parent lock base'
    $resolutions = [regex]::Matches($Text, '(?im)^\s*(?<variable>\$[A-Za-z_][A-Za-z0-9_]*)\s*=\s*Resolve-GitExecutablePath\s*$')
    Assert ($resolutions.Count -eq 1) 'trusted verifier must use exactly one resolved executable path for lock construction'
    $resolved = [regex]::Escape($resolutions[0].Groups['variable'].Value)
    Assert ($Text -match '(?is)hash-object.{0,1000}\$mergedLock.{0,1000}c4b4316368317a342058f77151ab8043afea676b.{0,1000}(?:Blob\s+\$candidateSha.{0,500}Cargo\.lock|hash-object.{0,500}\$candidateLock)') 'trusted verifier must hash emitted merge bytes to final lock blob before candidate comparison'
    Assert-NoPatchReplay $Text
    Assert-RawBinaryCaptureContract $Text
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
    Assert-CanonicalWorktreeOperation $text 'detached candidate worktree'
    Assert ($text -match 'git status --porcelain') 'workflow must require clean tracked trees'
    Assert-TrustedVerifierIdentity $text
    Assert ($text -match '(?m)&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File') 'trusted verifier must use the Ubuntu-compatible pwsh interpreter'
    Assert ($text -notmatch '(?im)&\s+powershell(?:\.exe)?\s+-NoProfile') 'trusted verifier must not use the Windows-only powershell interpreter'
    Assert-CleanMaterializationContract $text
    Assert-CandidateScrubAndCargoContract $text
    Assert-ExternalTargetContract $text
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
- uses: actions/checkout@v7
  with:
    path: trusted
$trustedRoot = Join-Path $env:GITHUB_WORKSPACE 'trusted'
$trustedVerifier = Join-Path $trustedRoot 'scripts/verify-package-candidate.ps1'
if (-not (Test-Path -LiteralPath $trustedVerifier)) { throw 'missing trusted verifier' }
git fetch origin $env:CANDIDATE_SHA
git worktree add --detach $env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA $env:CANDIDATE_SHA
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; Remove-Item Env:*; & pwsh -NoProfile -ExecutionPolicy Bypass -File $trustedVerifier -Candidate $env:CANDIDATE_SHA; Pop-Location
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; cargo metadata --locked; Pop-Location
Push-Location "$env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA"; cargo tree --locked; Pop-Location
'@
    Assert-TrustedVerifierIdentity $happy
    Assert-EnvBoundOperation $happy 'candidate fetch' '^.*git\s+fetch.*$'
    Assert-CanonicalWorktreeOperation $happy 'detached candidate worktree'
    Assert-SafeCandidateExecution $happy 'trusted verifier invocation' '&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\b'
    Assert-SafeCandidateExecution $happy 'candidate Cargo metadata invocation' 'cargo\s+metadata\s+--locked'
    Assert-SafeCandidateExecution $happy 'candidate Cargo tree invocation' 'cargo\s+tree\s+--locked'

    $capturedWorktreeHappy = @'
- name: Validate identity and materialize candidate
  run: |
    $candidateSha = $env:CANDIDATE_SHA
    $candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")
    git worktree add --detach $candidateWorktree $candidateSha
'@
    Assert-CanonicalWorktreeOperation $capturedWorktreeHappy 'detached candidate worktree'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy.Replace('$candidateSha', '$candidateAlias')) 'detached candidate worktree' } 'captured worktree form must reject SHA aliases'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy.Replace('$candidateWorktree $candidateSha', '$worktreeAlias $candidateSha')) 'detached candidate worktree' } 'captured worktree form must reject worktree aliases'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy.Replace('$candidateWorktree $candidateSha', '$env:RUNNER_TEMP/candidate-$candidateSha $candidateSha')) 'detached candidate worktree' } 'captured worktree form must reject noncanonical direct paths'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy.Replace('$candidateWorktree $candidateSha', '$candidateWorktree/child $candidateSha')) 'detached candidate worktree' } 'captured worktree form must reject child traversal targets'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy.Replace('$candidateWorktree $candidateSha', '$candidateWorktree $env:CANDIDATE_SHA')) 'detached candidate worktree' } 'captured worktree form must require the captured SHA argument'
    $outOfBlockCaptures = @'
$candidateSha = $env:CANDIDATE_SHA
$candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")
- name: Validate identity and materialize candidate
  run: |
    git worktree add --detach $candidateWorktree $candidateSha
'@
    Assert-Throws { Assert-CanonicalWorktreeOperation $outOfBlockCaptures 'detached candidate worktree' } 'captured worktree form must reject captures outside the named materialization block'
    $commentedWorktree = @'
# git worktree add --detach $env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA $env:CANDIDATE_SHA
$example = 'git worktree add --detach $env:RUNNER_TEMP/candidate-$env:CANDIDATE_SHA $env:CANDIDATE_SHA'
'@
    Assert-Throws { Assert-CanonicalWorktreeOperation $commentedWorktree 'detached candidate worktree' } 'comments and strings must not establish canonical worktree creation'
    $coexistingAlternates = @(
        'git worktree add --detach $worktreeAlias $candidateSha',
        'git worktree add --detach $env:RUNNER_TEMP/candidate-$candidateSha $candidateSha',
        'git worktree add --detach $candidateWorktree/child $candidateSha',
        'git worktree add --detach $candidateWorktree $env:CANDIDATE_SHA'
    )
    foreach ($alternate in $coexistingAlternates) {
        Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy + "`n    $alternate") 'detached candidate worktree' } "a valid captured worktree route must not authorize coexisting alternate route [$alternate]"
    }
    Assert-Throws { Assert-CanonicalWorktreeOperation ($capturedWorktreeHappy + "`n    git worktree add --detach `$candidateWorktree `$candidateSha") 'detached candidate worktree' } 'duplicate captured worktree-add operations must fail'
    Assert-Throws { Assert-CanonicalWorktreeOperation ($happy + "`n- name: Validate identity and materialize candidate`n  run: |`n    `$candidateSha = `$env:CANDIDATE_SHA`n    `$candidateWorktree = Join-Path `$env:RUNNER_TEMP (`"candidate-`$candidateSha`")`n    git worktree add --detach `$candidateWorktree `$candidateSha") 'detached candidate worktree' } 'valid inline worktree route must not authorize an extra captured route'

    $materializationRun = @'
$candidateSha = $env:CANDIDATE_SHA
$candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")
git worktree add --detach $candidateWorktree $candidateSha
Push-Location $candidateWorktree
$materializedCandidateHead = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "materialized HEAD failed: $materializedCandidateHead" }
if ($materializedCandidateHead -ne $candidateSha) { throw "materialized HEAD mismatch: $materializedCandidateHead" }
$materializedCandidatePorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "materialized status failed: $materializedCandidatePorcelain" }
if ($materializedCandidatePorcelain -ne '') { throw "materialized dirty: $materializedCandidatePorcelain" }
$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"
Pop-Location
'@
    $materializationHappy = "- name: Validate identity and materialize candidate`n  run: |`n" + (($materializationRun -split "`n" | ForEach-Object { '    ' + $_ }) -join "`n")
    Assert-CleanMaterializationContract $materializationHappy
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('git status --porcelain --untracked-files=all', 'git status --porcelain')) } 'materialization gate must require full untracked status'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('$materializedCandidatePorcelain = git status --porcelain --untracked-files=all', 'git status --porcelain --untracked-files=all | Out-Null')) } 'materialization gate must capture output'
    $genericMaterializationFailure = $materializationHappy.Replace('throw "materialized status failed: $materializedCandidatePorcelain"', "throw 'generic status failure'").Replace('throw "materialized dirty: $materializedCandidatePorcelain"', "throw 'generic dirty failure'")
    Assert-Throws { Assert-CleanMaterializationContract $genericMaterializationFailure } 'materialization failure must report captured porcelain'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('if ($materializedCandidatePorcelain -ne '''')', "if ('hard-coded' -ne '')")) } 'materialization gate must compare captured porcelain'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('if ($LASTEXITCODE -ne 0)', 'Write-Output masked')) } 'materialization native status failure must not be masked'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('Push-Location $candidateWorktree', '$candidateTarget = Join-Path $candidateWorktree target`nPush-Location $candidateWorktree')) } 'candidate-root setup before materialization proof must fail'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('$materializedCandidatePorcelain = git status --porcelain --untracked-files=all', "Set-Content -LiteralPath (Join-Path `$candidateWorktree 'residue') -Value residue`n`$materializedCandidatePorcelain = git status --porcelain --untracked-files=all")) } 'candidate writes before materialization clean proof must fail'
    Assert-Throws { Assert-CleanMaterializationContract ($materializationHappy.Replace('$materializedCandidatePorcelain = git status --porcelain --untracked-files=all', "[IO.File]::WriteAllText((Join-Path `$candidateWorktree 'residue'), 'residue')`n`$materializedCandidatePorcelain = git status --porcelain --untracked-files=all")) } 'IO candidate writes before materialization clean proof must fail'
    $outOfStepMaterialization = $materializationHappy.Replace('$materializedCandidatePorcelain = git status --porcelain --untracked-files=all', '$materializedStatusRemoved = $true') + "`n- uses: actions/checkout@v4`n  run: |`n    `$materializedCandidatePorcelain = git status --porcelain --untracked-files=all`n    if (`$LASTEXITCODE -ne 0) { throw `"late status: `$materializedCandidatePorcelain`" }`n    if (`$materializedCandidatePorcelain -ne '') { throw `"late dirty: `$materializedCandidatePorcelain`" }"
    Assert-Throws { Assert-CleanMaterializationContract $outOfStepMaterialization } 'later YAML steps cannot satisfy materialization clean proof'

    $candidateCargoHappy = @'
$candidateSha = $env:CANDIDATE_SHA
$candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")
$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"
$candidateTargetCanonical = [IO.Path]::GetFullPath($candidateTarget)
$candidateWorktreeCanonical = [IO.Path]::GetFullPath($candidateWorktree)
if ($candidateTargetCanonical -eq $candidateWorktreeCanonical) { throw 'target equals worktree' }
$toolPath = $env:PATH
$homePath = $env:HOME
$cargoHome = $env:CARGO_HOME
$rustupHome = $env:RUSTUP_HOME
$approvedCandidatePaths = "Cargo.lock`ncrates/chidori-js/Cargo.toml`ncrates/chidori/Cargo.toml"
Push-Location $candidateWorktree
try {
$preTargetPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "pre-target status failed: $preTargetPorcelain" }
if ($preTargetPorcelain -ne '') { throw "pre-target dirty: $preTargetPorcelain" }
$candidateHead = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'candidate HEAD evidence failed' }
$candidateStatus = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'candidate status evidence failed' }
$parents = (git show -s --format=%P $candidateSha).Split(' ')
if ($LASTEXITCODE -ne 0) { throw 'candidate parent evidence failed' }
if ($candidateHead -ne $candidateSha) { throw 'candidate HEAD mismatch' }
if ($candidateStatus -ne '') { throw 'candidate dirty' }
if ($parents.Count -ne 1) { throw 'candidate parent count failed' }
if ($parents[0] -ne '9e1c295b1f3f90ef342021ec81b52ece7363cd6c') { throw 'candidate base failed' }
$candidatePaths = git diff --name-only 9e1c295b1f3f90ef342021ec81b52ece7363cd6c $candidateSha
if ($LASTEXITCODE -ne 0) { throw 'candidate path evidence failed' }
$candidatePathText = ($candidatePaths -join "`n")
if ($candidatePathText -ne $approvedCandidatePaths) { throw 'candidate path scope failed' }
Remove-Item Env:*
$env:PATH = $toolPath
$env:HOME = $homePath
$env:CARGO_HOME = $cargoHome
$env:RUSTUP_HOME = $rustupHome
$env:CARGO_TARGET_DIR = $candidateTarget
& pwsh -NoProfile -ExecutionPolicy Bypass -File $trustedVerifier -Target $candidateTarget
if ($LASTEXITCODE -ne 0) { throw 'trusted verifier failed' }
$verifierPostPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "verifier-post status failed: $verifierPostPorcelain" }
if ($verifierPostPorcelain -ne '') { throw "verifier-post dirty: $verifierPostPorcelain" }
    $candidateHead = git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'post-scrub candidate HEAD evidence failed' }
    $candidateStatus = git status --porcelain
    if ($LASTEXITCODE -ne 0) { throw 'post-scrub candidate status evidence failed' }
    $parents = (git show -s --format=%P $candidateSha).Split(' ')
    if ($LASTEXITCODE -ne 0) { throw 'post-scrub candidate parent evidence failed' }
    if ($candidateHead -ne $candidateSha) { throw 'post-scrub candidate HEAD mismatch' }
    if ($candidateStatus -ne '') { throw 'post-scrub candidate dirty' }
    if ($parents.Count -ne 1) { throw 'post-scrub candidate parent count failed' }
    if ($parents[0] -ne '9e1c295b1f3f90ef342021ec81b52ece7363cd6c') { throw 'post-scrub candidate base failed' }
    $candidatePaths = git diff --name-only 9e1c295b1f3f90ef342021ec81b52ece7363cd6c $candidateSha
    if ($LASTEXITCODE -ne 0) { throw 'post-scrub candidate path evidence failed' }
    $candidatePathText = ($candidatePaths -join "`n")
    if ($candidatePathText -ne $approvedCandidatePaths) { throw 'post-scrub candidate path scope failed' }
    cargo metadata --locked --format-version 1 --no-deps
    if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
    cargo tree --locked -p chidori
    if ($LASTEXITCODE -ne 0) { throw 'cargo tree failed' }
    $candidateHead = git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'post-Cargo candidate HEAD evidence failed' }
    $candidateStatus = git status --porcelain
    if ($LASTEXITCODE -ne 0) { throw 'post-Cargo candidate status evidence failed' }
    $parents = (git show -s --format=%P $candidateSha).Split(' ')
    if ($LASTEXITCODE -ne 0) { throw 'post-Cargo candidate parent evidence failed' }
    if ($candidateHead -ne $candidateSha) { throw 'post-Cargo candidate HEAD mismatch' }
    if ($candidateStatus -ne '') { throw 'post-Cargo candidate dirty' }
    if ($parents.Count -ne 1) { throw 'post-Cargo candidate parent count failed' }
    if ($parents[0] -ne '9e1c295b1f3f90ef342021ec81b52ece7363cd6c') { throw 'post-Cargo candidate base failed' }
    $candidatePaths = git diff --name-only 9e1c295b1f3f90ef342021ec81b52ece7363cd6c $candidateSha
    if ($LASTEXITCODE -ne 0) { throw 'post-Cargo candidate path evidence failed' }
    $candidatePathText = ($candidatePaths -join "`n")
    if ($candidatePathText -ne $approvedCandidatePaths) { throw 'post-Cargo candidate path scope failed' }
} finally {
    Pop-Location
}
'@
    $candidateCargoWorkflow = "- name: Run locked Cargo checks in the candidate worktree`n  run: |`n" + (($candidateCargoHappy -split "`n" | ForEach-Object { '    ' + $_ }) -join "`n")
    Assert-CandidateScrubAndCargoContract $candidateCargoWorkflow
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$preTargetPorcelain = git status --porcelain --untracked-files=all', '$preTargetPorcelain = git status --porcelain')) } 'locked-Cargo pre-target gate must require full untracked status'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$preTargetPorcelain = git status --porcelain --untracked-files=all', 'git status --porcelain --untracked-files=all | Out-Null')) } 'locked-Cargo pre-target gate must capture output'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('if ($LASTEXITCODE -ne 0) { throw "pre-target status failed: $preTargetPorcelain" }', 'Write-Output masked')) } 'locked-Cargo pre-target native failure must not be masked'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$preTargetPorcelain = git status --porcelain --untracked-files=all', 'New-Item -ItemType Directory -Path $candidateWorktree/output`n$preTargetPorcelain = git status --porcelain --untracked-files=all')) } 'candidate-root setup before pre-target clean proof must fail'
    $genericPreTargetFailure = $candidateCargoWorkflow.Replace('throw "pre-target status failed: $preTargetPorcelain"', "throw 'generic pre-target failure'").Replace('throw "pre-target dirty: $preTargetPorcelain"', "throw 'generic pre-target dirty'")
    Assert-Throws { Assert-CandidateScrubAndCargoContract $genericPreTargetFailure } 'locked-Cargo pre-target failure must report captured porcelain'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('if ($preTargetPorcelain -ne '''')', "if ('hard-coded' -ne '')")) } 'locked-Cargo pre-target gate must compare captured porcelain'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('if ($preTargetPorcelain -ne '''')', "if (`$true) { throw 'dirty' }")) } 'locked-Cargo pre-target gate must use captured porcelain'
    Assert-NoCargoTargetDirFlags "cargo metadata --locked # --target-dir ignored`ncargo tree --locked"
    Assert-NoCargoTargetDirFlags "`$example = 'cargo metadata --locked --target-dir ignored'`ncargo metadata --locked`ncargo tree --locked"
    Assert-Throws { Assert-NoCargoTargetDirFlags 'cargo metadata --locked --target-dir output' } 'executed split target-dir flag must fail command-aware inspection'
    Assert-Throws { Assert-NoCargoTargetDirFlags 'cargo tree --locked --target-dir=output' } 'executed equals target-dir flag must fail command-aware inspection'
    $externalTargetHappy = @'
$candidateSha = $env:CANDIDATE_SHA
$candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")
$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"
$candidateTargetCanonical = [IO.Path]::GetFullPath($candidateTarget)
$candidateWorktreeCanonical = [IO.Path]::GetFullPath($candidateWorktree)
$separator = [IO.Path]::DirectorySeparatorChar
$alternateSeparator = [IO.Path]::AltDirectorySeparatorChar
if ($candidateTargetCanonical -eq $candidateWorktreeCanonical) { throw 'target equals worktree' }
if ($candidateTargetCanonical.StartsWith($candidateWorktreeCanonical + $separator) -or $candidateTargetCanonical.StartsWith($candidateWorktreeCanonical + $alternateSeparator)) { throw 'target inside worktree' }
$targetDirectory = [IO.Directory]::CreateDirectory($candidateTargetCanonical)
$verifierPrePorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "verifier-pre status failed: $verifierPrePorcelain" }
if ($verifierPrePorcelain -ne '') { throw "verifier-pre dirty: $verifierPrePorcelain" }
Remove-Item Env:*
$env:CARGO_TARGET_DIR = $candidateTarget
& pwsh -NoProfile -ExecutionPolicy Bypass -File $trustedVerifier -Target $candidateTarget
if ($LASTEXITCODE -ne 0) { throw 'verifier failed' }
$verifierPostPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "verifier-post status failed: $verifierPostPorcelain" }
if ($verifierPostPorcelain -ne '') { throw "verifier-post dirty: $verifierPostPorcelain" }
$outerPrePorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "outer-pre status failed: $outerPrePorcelain" }
if ($outerPrePorcelain -ne '') { throw "outer-pre dirty: $outerPrePorcelain" }
$outerPostScrubPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "outer-post-scrub status failed: $outerPostScrubPorcelain" }
if ($outerPostScrubPorcelain -ne '') { throw "outer-post-scrub dirty: $outerPostScrubPorcelain" }
cargo metadata --locked
if ($LASTEXITCODE -ne 0) { throw 'metadata failed' }
cargo tree --locked
if ($LASTEXITCODE -ne 0) { throw 'tree failed' }
$outerPostCargoPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "outer-post-Cargo status failed: $outerPostCargoPorcelain" }
if ($outerPostCargoPorcelain -ne '') { throw "outer-post-Cargo dirty: $outerPostCargoPorcelain" }
Remove-Item -LiteralPath $candidateTarget -Recurse -Force
if (Test-Path -LiteralPath $candidateTarget) { throw 'target cleanup failed' }
$finalPostTargetCleanupPorcelain = git status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0) { throw "final-post-target-cleanup status failed: $finalPostTargetCleanupPorcelain" }
if ($finalPostTargetCleanupPorcelain -ne '') { throw "final-post-target-cleanup dirty: $finalPostTargetCleanupPorcelain" }
'@
    $externalTargetWorkflow = "- name: Run locked Cargo checks in the candidate worktree`n  run: |`n" + (($externalTargetHappy -split "`n" | ForEach-Object { '    ' + $_ }) -join "`n")
    Assert-ExternalTargetContract $externalTargetWorkflow
    foreach ($phase in @('verifierPre', 'verifierPost', 'outerPre', 'outerPostScrub', 'outerPostCargo', 'finalPostTargetCleanup')) {
        $variable = '$' + $phase + 'Porcelain'
        $label = switch ($phase) { 'verifierPre' { 'verifier-pre' } 'verifierPost' { 'verifier-post' } 'outerPre' { 'outer-pre' } 'outerPostScrub' { 'outer-post-scrub' } 'outerPostCargo' { 'outer-post-Cargo' } default { 'final-post-target-cleanup' } }
        Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace("$variable = git status --porcelain --untracked-files=all", "$variable = git status --porcelain")) } "[$phase] missing full status mode must fail"
        Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace("$variable = git status --porcelain --untracked-files=all", 'git status --porcelain --untracked-files=all | Out-Null')) } "[$phase] discarded status output must fail"
        Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace("$variable = git status --porcelain --untracked-files=all", "$variable = ''")) } "[$phase] omitted status capture must fail"
        $genericStatus = $externalTargetWorkflow.Replace(('throw "' + $label + ' status failed: ' + $variable + '"'), "throw 'generic status failure'").Replace(('throw "' + $label + ' dirty: ' + $variable + '"'), "throw 'generic dirty failure'")
        Assert-Throws { Assert-ExternalTargetContract $genericStatus } "[$phase] generic status evidence must fail"
        Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace("if ($variable -ne '')", "if ('hard-coded' -ne '')")) } "[$phase] hard-coded status comparison must fail"
        $maskedStatus = $externalTargetWorkflow.Replace(('if ($LASTEXITCODE -ne 0) { throw "' + $label + ' status failed: ' + $variable + '" }'), "Write-Output 'later success masks status failure'")
        Assert-Throws { Assert-ExternalTargetContract $maskedStatus } "[$phase] delayed or masked status exit evidence must fail"
        $phaseBlock = [regex]::Match($externalTargetWorkflow, '(?ms)^[ \t]*\$' + $phase + 'Porcelain\s*=.*?^[ \t]*if \(\$' + $phase + 'Porcelain\s*-ne\s*''''\) \{.*?\}').Value
        Assert ($phaseBlock -ne '') "fixture phase block [$phase] must be found before relocation"
        $withoutPhase = $externalTargetWorkflow.Replace($phaseBlock, '')
        $relocatedPhase = if ($phase -eq 'finalPostTargetCleanup') { $phaseBlock + $withoutPhase } else { $withoutPhase + $phaseBlock }
        Assert-Throws { Assert-ExternalTargetContract $relocatedPhase } "[$phase] status phase outside its required placement must fail"
    }
    $postCleanBlock = [regex]::Match($externalTargetWorkflow, '(?ms)^[ \t]*\$finalPostTargetCleanupPorcelain\s*=.*?^[ \t]*if \(\$finalPostTargetCleanupPorcelain\s*-ne\s*''''\) \{.*?\}').Value
    Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace($postCleanBlock, '')) } 'missing final post-clean candidate status must fail'
    Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace('$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"', '$candidateTarget = $candidateWorktree')) } 'target inside candidate worktree must fail'
    Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace('Remove-Item -LiteralPath $candidateTarget', 'git clean -fd')) } 'broad cleanup must fail'
    Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace('Remove-Item -LiteralPath $candidateTarget -Recurse -Force', 'Remove-Item -LiteralPath $candidateTarget')) } 'nonrecursive cleanup of nonempty target must fail'
    Assert-Throws { Assert-ExternalTargetContract ($externalTargetWorkflow.Replace('Remove-Item -LiteralPath $candidateTarget -Recurse -Force', 'Remove-Item -LiteralPath $candidateWorktree -Recurse -Force')) } 'candidate-root cleanup must fail'
    $targetCapture = '$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"'
    $disconnectedTarget = $targetCapture + "`n" + $externalTargetWorkflow.Replace($targetCapture, '# disconnected earlier target capture')
    Assert-Throws { Assert-ExternalTargetContract $disconnectedTarget } 'disconnected earlier-step target capture must not satisfy the locked Cargo block'
    $postScrubTarget = $externalTargetWorkflow.Replace($targetCapture, '').Replace('Remove-Item Env:*', "Remove-Item Env:*`n    $targetCapture")
    Assert-Throws { Assert-ExternalTargetContract $postScrubTarget } 'post-scrub target derivation must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', 'cargo metadata --locked --target-dir $candidateTarget')) } 'split target-dir metadata flag must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo tree --locked -p chidori', 'cargo tree --locked --target-dir=$candidateTarget -p chidori')) } 'equals target-dir tree flag must fail'
    $secondNoCargoScope = $candidateCargoWorkflow.Replace("run: |`n", "run: |`n    Push-Location `$candidateWorktree`n    try { `$null = `$null } finally { Pop-Location }`n")
    Assert-Throws { Assert-CandidateScrubAndCargoContract $secondNoCargoScope } 'second no-Cargo candidate scope must fail'
    foreach ($prePushEvidence in @(
        '$candidateHead = git rev-parse HEAD',
        '$candidateStatus = git status --porcelain',
        '$parents = (git show -s --format=%P $candidateSha).Split('' '')',
        '$candidatePaths = git diff --name-only 9e1c295b1f3f90ef342021ec81b52ece7363cd6c $candidateSha'
    )) {
        $prePush = $candidateCargoWorkflow.Replace("run: |`n", "run: |`n    $prePushEvidence`n    if (`$LASTEXITCODE -ne 0) { throw 'pre-push evidence failed' }`n")
        Assert-Throws { Assert-CandidateScrubAndCargoContract $prePush } "candidate evidence before canonical Push-Location must fail: $prePushEvidence"
    }
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('Push-Location $candidateWorktree', 'Push-Location trusted')) } 'pre-Push trusted/default-CWD evidence must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('Push-Location $candidateWorktree', '')) } 'pre-Push evidence must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('Remove-Item Env:*', 'Pop-Location`nRemove-Item Env:*')) } 'split candidate scopes must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace("if (`$LASTEXITCODE -ne 0) { throw 'candidate HEAD evidence failed' }", 'Pop-Location')) } 'Pop-Location must not mask pre-scrub native failure'
    $approvedPathLines = @('Cargo.lock', 'crates/chidori-js/Cargo.toml', 'crates/chidori/Cargo.toml')
    $approvedPathText = ($approvedPathLines -join "`n")
    Assert ((@('Cargo.lock', 'crates/chidori-js/Cargo.toml', 'crates/chidori/Cargo.toml') -join "`n") -eq $approvedPathText) 'ordered approved candidate paths must normalize to the expected scalar'
    foreach ($badPaths in @(@('crates/chidori-js/Cargo.toml', 'Cargo.lock', 'crates/chidori/Cargo.toml'), @('Cargo.lock', 'crates/chidori-js/Cargo.toml'), @('Cargo.lock', 'crates/chidori-js/Cargo.toml', 'crates/chidori/Cargo.toml', 'extra'))) {
        Assert (($badPaths -join "`n") -ne $approvedPathText) 'reordered, missing, or extra candidate paths must fail ordered scalar comparison'
    }
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidateWorktree = Join-Path $env:RUNNER_TEMP ("candidate-$candidateSha")', '$candidateWorktree = $env:RUNNER_TEMP')) } 'missing canonical candidate worktree capture must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:PATH = $toolPath', '$env:CANDIDATE_SHA = $candidateSha')) } 'unsafe environment restoration must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', '$env:ARBITRARY = $candidateTarget')) } 'arbitrary scrubbed environment restore must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', '$env:CARGO_TARGET_DIR = (Join-Path $env:RUNNER_TEMP "derived-target")')) } 'derived target restore must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', '$env:CARGO_TARGET_DIR = $candidateWorktree')) } 'alternate target restore must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', "`$env:CARGO_TARGET_DIR = `$candidateTarget`n    `$env:CARGO_TARGET_DIR = `$candidateTarget")) } 'duplicate target restore must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', "`$env:CARGO_TARGET_DIR = `$candidateTarget`n    `$env:CARGO_TARGET_DIR = `$candidateWorktree")) } 'second arbitrary target restore after the valid restore must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$env:CARGO_TARGET_DIR = $candidateTarget', "`$env:CARGO_TARGET_DIR = `$candidateTarget`n    `$env:CARGO_TARGET_DIR = (Join-Path `$env:RUNNER_TEMP 'derived-target')")) } 'second derived target restore after the valid restore must fail'
    $postScrubTargetDerivation = 'Remove-Item Env:*' + "`n    " + '$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidateTarget = Join-Path $env:RUNNER_TEMP "candidate-target-$candidateSha"', '').Replace('Remove-Item Env:*', $postScrubTargetDerivation)) } 'post-scrub target derivation must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('Push-Location $candidateWorktree', '& pwsh -NoProfile -ExecutionPolicy Bypass -File $trustedVerifier -Target $candidateTarget`nPush-Location $candidateWorktree')) } 'verifier outside the canonical candidate scope must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow + "`n    Push-Location `$candidateWorktree`n    try { & pwsh -NoProfile -ExecutionPolicy Bypass -File `$trustedVerifier -Target `$candidateTarget } finally { Pop-Location }") } 'second verifier candidate scope must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('Push-Location $candidateWorktree', 'Push-Location trusted')) } 'trusted or host worktree execution must fail'
    $longDistance = $candidateCargoWorkflow.Replace("    `$candidateHead = git rev-parse HEAD", ("    `$padding = '" + ('x' * 2101) + "'`n    `$candidateHead = git rev-parse HEAD"))
    Assert-CandidateScrubAndCargoContract $longDistance
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', 'cargo metadata --locked --format-version 1 --no-deps`n    cargo metadata --locked --format-version 1 --no-deps').Replace('$verifierPostPorcelain = git status --porcelain --untracked-files=all', 'cargo tree --locked -p chidori`n    $verifierPostPorcelain = git status --porcelain --untracked-files=all')) } 'Cargo before required verifier-post phase must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$verifierPostPorcelain = git status --porcelain --untracked-files=all', '$null = git status --porcelain --untracked-files=all')) } 'missing verifier-post phase must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', '$cargoText = ''cargo metadata --locked --format-version 1 --no-deps''')) } 'string-only Cargo text must not satisfy literal metadata execution'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', '# cargo metadata --locked --format-version 1 --no-deps')) } 'comment-only Cargo text must not satisfy literal metadata execution'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', '& $cargoExecutable metadata --locked --format-version 1 --no-deps')) } 'variable-backed Cargo executable must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', 'cargo @cargoArguments')) } 'splatted Cargo metadata must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('cargo metadata --locked --format-version 1 --no-deps', 'Start-Process cargo -ArgumentList ''metadata'', ''--locked''')) } 'process-launch Cargo bypass must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('if ($LASTEXITCODE -ne 0) { throw ''cargo metadata failed'' }', 'Write-Host masked')) } 'delayed Cargo failure check must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace("if (`$LASTEXITCODE -ne 0) { throw 'candidate parent evidence failed' }", 'Write-Host masked')) } 'delayed native evidence failure check must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace("if (`$LASTEXITCODE -ne 0) { throw 'post-scrub candidate parent evidence failed' }", 'Write-Host masked')) } 'missing post-scrub parent evidence failure check must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace("if (`$LASTEXITCODE -ne 0) { throw 'post-Cargo candidate parent evidence failed' }", 'Write-Host masked')) } 'missing post-Cargo parent evidence failure check must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidateSha = $env:CANDIDATE_SHA', '# candidate SHA captured in an earlier step')) } 'workflow-global or earlier-step candidate capture must not satisfy the locked Cargo block'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidateHead = git rev-parse HEAD', 'git rev-parse HEAD | Out-Null')) } 'Out-Null evidence must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidateHead -ne $candidateSha', '$candidateHead -ne ''hard-coded''')) } 'hard-coded expected identity must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow -replace '\$candidateStatus\s*-ne\s*''''', '$true') } 'captured output must not be unused'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow -replace '(?ms)^\s*\$candidatePaths\s*=.*?^\s*if \(\$candidatePathText -ne \$approvedCandidatePaths\).*?\r?\n', '') } 'missing path comparison must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidatePathText -ne $approvedCandidatePaths', '$candidatePathText -ne ''hard-coded-paths''')) } 'hard-coded path comparison must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidatePathText -ne $approvedCandidatePaths', '$true')) } 'unused normalized path scalar must fail'
    Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace('$candidatePathText = ($candidatePaths -join "`n")', '$candidatePathText = (($candidatePaths | Sort-Object) -join "`n")')) } 'sorted path normalization must fail'
    foreach ($phase in @('post-scrub', 'post-Cargo')) {
        foreach ($comparison in @("if (`$candidateHead -ne `$candidateSha) { throw '$phase candidate HEAD mismatch' }", "if (`$candidateStatus -ne '') { throw '$phase candidate dirty' }", "if (`$parents.Count -ne 1) { throw '$phase candidate parent count failed' }", "if (`$parents[0] -ne '9e1c295b1f3f90ef342021ec81b52ece7363cd6c') { throw '$phase candidate base failed' }", "if (`$candidatePathText -ne `$approvedCandidatePaths) { throw '$phase candidate path scope failed' }")) {
            Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace($comparison, 'Write-Host removed')) } "$phase phase must reject a removed evidence comparison"
            Assert-Throws { Assert-CandidateScrubAndCargoContract ($candidateCargoWorkflow.Replace($comparison, "if (`$true) { throw 'hard-coded' }")) } "$phase phase must reject a hard-coded evidence comparison"
        }
    }
    $bleed = $candidateCargoWorkflow.Replace('$candidatePaths = git diff', '# removed path capture`n$null = git diff') + "`n  - uses: actions/checkout@v7`n    run: |`n      `$candidatePaths = git diff --name-only 9e1c295b1f3f90ef342021ec81b52ece7363cd6c `$candidateSha"
    Assert-Throws { Assert-CandidateScrubAndCargoContract $bleed } 'unnamed sibling step must not bleed evidence into locked Cargo block'

    $runnerTempTrusted = $happy.Replace('$env:GITHUB_WORKSPACE', '$env:RUNNER_TEMP')
    Assert-Throws { Assert-TrustedVerifierIdentity $runnerTempTrusted } 'RUNNER_TEMP trusted verifier root must be rejected'

    $checkoutRootMismatch = $happy.Replace("Join-Path `$env:GITHUB_WORKSPACE 'trusted'", "Join-Path `$env:GITHUB_WORKSPACE 'other'")
    Assert-Throws { Assert-TrustedVerifierIdentity $checkoutRootMismatch } 'trusted checkout and trusted root mismatch must be rejected'

    $missingVerifierCheck = [regex]::Replace($happy, '(?m)^if \(-not \(Test-Path -LiteralPath \$trustedVerifier\)\) \{ throw ''missing trusted verifier'' \}\r?\n?', '')
    Assert-Throws { Assert-TrustedVerifierIdentity $missingVerifierCheck } 'trusted verifier invocation without Test-Path must be rejected'

    $bareVerifierCheck = $happy.Replace("if (-not (Test-Path -LiteralPath `$trustedVerifier)) { throw 'missing trusted verifier' }", 'Test-Path -LiteralPath $trustedVerifier')
    Assert-Throws { Assert-TrustedVerifierIdentity $bareVerifierCheck } 'bare/no-op trusted verifier Test-Path must be rejected'

    $bareExitGuard = $happy.Replace("throw 'missing trusted verifier'", 'exit')
    Assert-Throws { Assert-TrustedVerifierIdentity $bareExitGuard } 'bare exit trusted verifier guard must be rejected'

    $zeroExitGuard = $happy.Replace("throw 'missing trusted verifier'", 'exit 0')
    Assert-Throws { Assert-TrustedVerifierIdentity $zeroExitGuard } 'exit 0 trusted verifier guard must be rejected'

    $directVerifierPath = $happy.Replace('-File $trustedVerifier', "-File (Join-Path `$trustedRoot 'scripts/verify-package-candidate.ps1')")
    Assert-Throws { Assert-TrustedVerifierIdentity $directVerifierPath } 'direct trusted verifier path must not bypass the canonical variable'

    $inertAlias = @'
$env:CANDIDATE_SHA
git fetch origin $candidate_alias
git worktree add --detach candidate $candidate_alias
powershell -File scripts/verify-package-candidate.ps1 -Candidate $candidate_alias
Set-Location "candidate-$candidate_alias"; cargo metadata --locked
Set-Location "candidate-$candidate_alias"; cargo tree --locked
'@
    Assert-Throws { Assert-EnvBoundOperation $inertAlias 'candidate fetch' '^.*git\s+fetch.*$' } 'inert earlier env reference must not authorize alias-backed fetch'
    Assert-Throws { Assert-CanonicalWorktreeOperation $inertAlias 'detached candidate worktree' } 'inert earlier env reference must not authorize alias-backed worktree'
    Assert-Throws { Assert-SafeCandidateExecution $inertAlias 'trusted verifier invocation' '&\s+pwsh\s+-NoProfile\s+-ExecutionPolicy\s+Bypass\s+-File\s+\$trustedVerifier\b' } 'inert earlier env reference must not authorize alias-backed verifier'
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
    Assert-CanonicalWorktreeOperation $mismatchedWorktree 'detached candidate worktree'
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

function Select-DeterministicGitCandidate([object[]]$Candidates) {
    $seen = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($candidate in $Candidates) {
        $raw = if (-not [string]::IsNullOrWhiteSpace($candidate.Source)) { $candidate.Source } else { $candidate.Path }
        if ([string]::IsNullOrWhiteSpace($raw) -or -not $candidate.Valid -or -not $candidate.Leaf) { continue }
        $canonical = $candidate.Canonical
        if ([string]::IsNullOrWhiteSpace($canonical)) { continue }
        if ($seen.Add($canonical)) { return $canonical }
    }
    throw 'no valid git Application executable path'
}

function Invoke-VerifierPortabilityFixtures {
    $portable = @'
function Resolve-GitExecutablePath {
    $seen = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($candidate in (Get-Command git -All -CommandType Application)) {
        $raw = if (-not [string]::IsNullOrWhiteSpace($candidate.Source)) { $candidate.Source } else { $candidate.Path }
        if ([string]::IsNullOrWhiteSpace($raw)) { continue }
        if (-not (Test-Path -LiteralPath $raw -PathType Leaf)) { continue }
        $canonical = (Resolve-Path -LiteralPath ([IO.Path]::GetFullPath($raw))).Path
        if ($seen.Add($canonical)) { return $canonical }
    }
    throw 'no valid git Application executable path'
}
$gitExecutablePath = Resolve-GitExecutablePath
function Invoke-ResolvedGit([string[]]$Arguments) {
    $out = & $gitExecutablePath @Arguments
}
$process = Start-Process -FilePath $gitExecutablePath -ArgumentList @('show', 'HEAD:Cargo.lock') -RedirectStandardOutput lock -NoNewWindow -PassThru -Wait
$mergeProcess = Start-Process -FilePath $gitExecutablePath -ArgumentList @('merge-file', '-p', '--', 'package', 'base', 'ctutils') -RedirectStandardOutput merged -NoNewWindow -PassThru -Wait
'@
    Assert-VerifierScriptPortability $portable
    Assert-VerifierScriptRecursionSafety $portable

    $same = Select-DeterministicGitCandidate @(
        [pscustomobject]@{ Source = 'git-a'; Path = ''; Canonical = 'C:\Git\git.exe'; Valid = $true; Leaf = $true },
        [pscustomobject]@{ Source = ''; Path = 'git-link'; Canonical = 'C:\Git\git.exe'; Valid = $true; Leaf = $true }
    )
    Assert ($same -eq 'C:\Git\git.exe') 'equivalent or symlink Application candidates must de-duplicate to one canonical path'
    $first = Select-DeterministicGitCandidate @(
        [pscustomobject]@{ Source = 'first'; Path = ''; Canonical = 'C:\Git\first.exe'; Valid = $true; Leaf = $true },
        [pscustomobject]@{ Source = 'second'; Path = ''; Canonical = 'D:\Git\second.exe'; Valid = $true; Leaf = $true }
    )
    Assert ($first -eq 'C:\Git\first.exe') 'distinct Application candidates must select the first valid command/PATH entry deterministically'
    Assert-Throws { Select-DeterministicGitCandidate @([pscustomobject]@{ Source = ''; Path = ''; Canonical = ''; Valid = $false; Leaf = $false }) } 'invalid or missing Application candidates must fail closed'
    Assert-Throws { Select-DeterministicGitCandidate @() } 'no Application candidates must fail closed'
    Assert-Throws { Select-DeterministicGitCandidate @([pscustomobject]@{ Source = 'C:\Git'; Path = ''; Canonical = 'C:\Git'; Valid = $true; Leaf = $false }) } 'nonempty directory or non-leaf Application candidate must not be selected'
    Assert-Throws { Select-DeterministicGitCandidate @([pscustomobject]@{ Source = 'C:\missing\git.exe'; Path = ''; Canonical = 'C:\missing\git.exe'; Valid = $false; Leaf = $false }) } 'nonempty missing Application candidate must not be selected'

    $collectionCoercion = $portable.Replace('$gitExecutablePath = Resolve-GitExecutablePath', '$gitExecutablePath = (Get-Command git -All -CommandType Application).Source')
    Assert-Throws { Assert-VerifierScriptRecursionSafety $collectionCoercion } 'collection Source/Path coercion must be rejected'

    foreach ($nonPortable in @(
        'git.exe status',
        'GIT.ExE status',
        '& git.exe status',
        '& GIT.EXE status',
        "& 'git.exe' status",
        "& 'GIT.ExE' status",
        'cmd /c git status',
        'cmd.exe /c git status',
        'CMD.ExE /c git status',
        '& cmd /c git status',
        '& CMD.EXE /c git status',
        "& 'cmd' /c git status",
        "& 'cmd.exe' /c git status",
        "& 'CMD.ExE' /c git status"
    )) {
        Assert-Throws { Assert-VerifierScriptPortability ($portable + "`n" + $nonPortable) } "non-portable verifier invocation must be rejected: $nonPortable"
    }

    $commentOnlyBareGit = @'
# git status --porcelain
  # & git apply --no-index patch
'@
    Assert-Throws { Assert-VerifierScriptRecursionSafety $commentOnlyBareGit } 'comment-only bare git must not satisfy resolved Application requirement'

    $portableWithWindowsComments = $portable + @'

# git.exe status
# cmd /c git status
  # & 'cmd.exe' /c git status
'@
    Assert-VerifierScriptPortability $portableWithWindowsComments

    foreach ($recursionUnsafe in @(
        "function Git { & `$gitExecutablePath status }",
        "function gIt { & `$gitExecutablePath status }",
        '& git status',
        'Start-Process -FilePath git -ArgumentList @(''status'')'
    )) {
        Assert-Throws { Assert-VerifierScriptRecursionSafety ($portable + "`n" + $recursionUnsafe) } "recursion-unsafe verifier form must be rejected: $recursionUnsafe"
    }

    $rawApplicationInvocation = @'
$gitApplication = Get-Command git -CommandType Application
& $gitApplication status
'@
    Assert-Throws { Assert-VerifierScriptRecursionSafety $rawApplicationInvocation } 'raw ApplicationInfo invocation must be rejected'
    $rawApplicationStartProcess = @'
$gitApplication = Get-Command git -CommandType Application
Start-Process -FilePath $gitApplication -ArgumentList @('status')
'@
    Assert-Throws { Assert-VerifierScriptRecursionSafety $rawApplicationStartProcess } 'raw ApplicationInfo Start-Process must be rejected'

    $replayCommentsAndStrings = $portable + "`n" + @'
# & $gitExecutablePath apply --no-index patch
$description = 'git diff --binary is forbidden only when executed'
Write-Host "git am is not a command here"
'@
    Assert-NoPatchReplay $replayCommentsAndStrings
    $replayVectors = @(
        "@('apply', '--no-index', 'patch')",
        "@('patch', 'patchfile')",
        "@('am', 'mailbox')",
        "@('diff', '--binary', 'HEAD')"
    )
    foreach ($vector in $replayVectors) {
        foreach ($replay in @(
            "Invoke-ResolvedGit -Arguments $vector",
            "`$gitArguments = $vector`n& `$gitExecutablePath `$gitArguments",
            "`$gitArguments = $vector`nStart-Process -FilePath `$gitExecutablePath -ArgumentList `$gitArguments",
            "`$gitSplat = @{ Arguments = $vector }`nInvoke-ResolvedGit @gitSplat",
            "`$processSplat = @{ FilePath = `$gitExecutablePath; ArgumentList = $vector }`nStart-Process @processSplat"
        )) {
            Assert-Throws { Assert-NoPatchReplay ($portable + "`n" + $replay) } "replay command must be rejected: $replay"
        }
    }
}

function Write-RawGitOutput([string]$Path, [string[]]$Arguments) {
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $script:RawCaptureGitExecutable
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $file = [System.IO.FileStream]::new($Path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try {
        Assert $process.Start() 'raw fixture Git process must start'
        $process.StandardOutput.BaseStream.CopyTo($file)
        $file.Flush($true)
        $process.WaitForExit()
        $stderr = $process.StandardError.ReadToEnd()
        if ($process.ExitCode -ne 0) { throw "raw fixture git $($Arguments -join ' ') failed: $stderr" }
    } finally {
        $file.Close()
        $process.Close()
    }
}

function Get-FixtureGitHash([string]$Path) {
    $hash = (& git.exe hash-object $Path | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'fixture hash-object failed' }
    return $hash
}

function Assert-FixtureMergedHash([string]$Package, [string]$Base, [string]$Ctutils, [string]$Output, [string]$ExpectedHash) {
    Write-RawGitOutput $Output @('merge-file', '-p', '--', $Package, $Base, $Ctutils)
    Assert ((Get-FixtureGitHash $Output) -eq $ExpectedHash) 'materialized three-way merge bytes did not match the required final lock blob'
}

function Invoke-LockCompositionFixtures {
    $fixtureResult = Get-RawImmutableFixtureResult ($null -ne [System.Diagnostics.ProcessStartInfo].GetProperty('ArgumentList'))
    if ($fixtureResult.State -eq 'PENDING') {
        $script:RawBinaryFixturePending = $true
        $script:RawBinaryFixturePendingReason = 'ProcessStartInfo.ArgumentList is unavailable in Windows PowerShell; pwsh/.NET Core is required to execute raw immutable-blob capture fixtures.'
        Write-Warning "PENDING: $script:RawBinaryFixturePendingReason"
    } else {
        $temp = Join-Path ([IO.Path]::GetTempPath()) ('chidori-lock-composition-' + [Guid]::NewGuid())
        New-Item -ItemType Directory -Path $temp | Out-Null
        try {
        $script:RawCaptureGitExecutable = (Get-Command git -CommandType Application | Select-Object -First 1).Source
        Assert (-not [string]::IsNullOrWhiteSpace($script:RawCaptureGitExecutable)) 'fixture must resolve a Git Application executable'
        $baseLock = Join-Path $temp 'base.lock'
        $packageLock = Join-Path $temp 'package.lock'
        $ctutilsLock = Join-Path $temp 'ctutils.lock'
        $mergedLock = Join-Path $temp 'merged.lock'
        $candidateLock = Join-Path $temp 'candidate.lock'
        Write-RawGitOutput $baseLock @('show', '9e1c295:Cargo.lock')
        Write-RawGitOutput $packageLock @('show', '59ebb109:Cargo.lock')
        Write-RawGitOutput $ctutilsLock @('show', '00148aa:Cargo.lock')
        Write-RawGitOutput $candidateLock @('show', 'c4b4316368317a342058f77151ab8043afea676b')
        Assert ((Get-FixtureGitHash $baseLock) -eq '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9') 'raw base snapshot must retain its source hash'
        Assert ((Get-FixtureGitHash $packageLock) -eq '9e36a2e89363eeedc4215a68dbd51f33dfec3741') 'raw package snapshot must retain its source hash'
        Assert ((Get-FixtureGitHash $ctutilsLock) -eq 'fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66') 'raw ctutils snapshot must retain its source hash'
        Assert ((Get-FixtureGitHash $candidateLock) -eq 'c4b4316368317a342058f77151ab8043afea676b') 'raw candidate snapshot must retain its source hash'
        Assert-FixtureMergedHash $packageLock $baseLock $ctutilsLock $mergedLock 'c4b4316368317a342058f77151ab8043afea676b'
        Assert ((Get-FixtureGitHash $mergedLock) -ne 'd5de22a66677f314498279b13e89de1aefec72f6') 'raw merge must reject the text-normalized lock blob'
        Assert ([System.Linq.Enumerable]::SequenceEqual([IO.File]::ReadAllBytes($mergedLock), [IO.File]::ReadAllBytes($candidateLock))) 'raw merge bytes must equal candidate lock bytes before candidate comparison'
        $normalizedLock = Join-Path $temp 'normalized.lock'
        $rawText = [Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($mergedLock))
        [IO.File]::WriteAllText($normalizedLock, ($rawText -replace "`n", "`r`n"), [Text.UTF8Encoding]::new($true))
        Assert ((Get-FixtureGitHash $normalizedLock) -ne 'c4b4316368317a342058f77151ab8043afea676b') 'CRLF/BOM text normalization must not preserve raw lock bytes'
        [IO.File]::AppendAllText($normalizedLock, "`n", [Text.UTF8Encoding]::new($false))
        Assert ((Get-FixtureGitHash $normalizedLock) -ne 'c4b4316368317a342058f77151ab8043afea676b') 'trailing-newline normalization must not preserve raw lock bytes'
        Assert-Throws { Assert-FixtureMergedHash $baseLock $packageLock $ctutilsLock $mergedLock 'c4b4316368317a342058f77151ab8043afea676b' } 'wrong package/base/ctutils merge order must fail final-byte identity'
        Assert-Throws { Assert-FixtureMergedHash $packageLock $ctutilsLock $baseLock $mergedLock 'c4b4316368317a342058f77151ab8043afea676b' } 'swapped base and ctutils snapshots must fail final-byte identity'
        Write-RawGitOutput $baseLock @('show', '59ebb109:Cargo.lock')
        Assert-Throws { Assert-FixtureMergedHash $packageLock $baseLock $ctutilsLock $mergedLock 'c4b4316368317a342058f77151ab8043afea676b' } 'wrong base snapshot must fail final-byte identity'
        Write-RawGitOutput $baseLock @('show', '9e1c295:Cargo.lock')
        Write-RawGitOutput $ctutilsLock @('show', '9e1c295:Cargo.lock')
        Assert-Throws { Assert-FixtureMergedHash $packageLock $baseLock $ctutilsLock $mergedLock 'c4b4316368317a342058f77151ab8043afea676b' } 'wrong ctutils target snapshot must fail final-byte identity'
        Write-RawGitOutput $ctutilsLock @('show', '00148aa:Cargo.lock')
        Assert-Throws { Assert-FixtureMergedHash $packageLock $baseLock $ctutilsLock $mergedLock ('0' * 40) } 'wrong expected final lock hash must fail equality'

        $overlapBase = Join-Path $temp 'overlap-base.lock'; $overlapPackage = Join-Path $temp 'overlap-package.lock'; $overlapCtutils = Join-Path $temp 'overlap-ctutils.lock'
        [IO.File]::WriteAllText($overlapBase, "same`n")
        [IO.File]::WriteAllText($overlapPackage, "package`n")
        [IO.File]::WriteAllText($overlapCtutils, "ctutils`n")
        Assert-Throws { Write-RawGitOutput $mergedLock @('merge-file', '-p', '--', $overlapPackage, $overlapBase, $overlapCtutils) } 'overlapping materialized snapshots must fail closed'
        } finally {
            if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force }
        }
    }

    $resolverHappy = @'
function Resolve-GitExecutablePath {
    $seen = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($candidate in (Get-Command git -All -CommandType Application)) {
        $raw = if (-not [string]::IsNullOrWhiteSpace($candidate.Source)) { $candidate.Source } else { $candidate.Path }
        if ([string]::IsNullOrWhiteSpace($raw)) { continue }
        if (-not (Test-Path -LiteralPath $raw -PathType Leaf)) { continue }
        $canonical = (Resolve-Path -LiteralPath ([IO.Path]::GetFullPath($raw))).Path
        if ($seen.Add($canonical)) { return $canonical }
    }
    throw 'no valid git Application executable path'
}
$gitExecutablePath = Resolve-GitExecutablePath
function Invoke-ResolvedGit([string[]]$Arguments) { $out = & $gitExecutablePath @Arguments }
'@
    $rawCaptureHappy = @'
function Write-RawGitOutput([string]$Path, [string[]]$Arguments) {
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $gitExecutablePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $file = [System.IO.FileStream]::new($Path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try {
        if (-not $process.Start()) { throw 'start failed' }
        $process.StandardOutput.BaseStream.CopyTo($file)
        $file.Flush($true)
        $process.WaitForExit()
        if ($process.ExitCode -ne 0) { throw 'capture failed' }
    } finally {
        $file.Close()
        $process.Close()
    }
}
'@
    $compositionHappy = @'
$packageCommit = '59ebb109'
$ctutilsCommit = '00148aa'
$directPaths = Invoke-ResolvedGit @('show', '--format=', '--name-only', $packageCommit, 'Cargo.lock')
$ctutilsPaths = Invoke-ResolvedGit @('show', '--format=', '--name-only', $ctutilsCommit, 'Cargo.lock')
Assert-Equal $directPaths $paths
Assert-Equal $ctutilsPaths Cargo.lock
Blob "$packageCommit^" Cargo.lock 47fbe5113e4b7c18cf2cc96f3a0410f045a281a9
Blob "$ctutilsCommit^" Cargo.lock 47fbe5113e4b7c18cf2cc96f3a0410f045a281a9
9e36a2e89363eeedc4215a68dbd51f33dfec3741
fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66
Write-RawGitOutput $packageLock @('show', "$packageCommit`:Cargo.lock")
Write-RawGitOutput $baseLock @('show', "$base`:Cargo.lock")
Write-RawGitOutput $ctutilsLock @('show', "$ctutilsCommit`:Cargo.lock")
Write-RawGitOutput $candidateLock @('show', "$candidateSha`:Cargo.lock")
Assert-Equal (Invoke-ResolvedGit @('hash-object', $packageLock)) '9e36a2e89363eeedc4215a68dbd51f33dfec3741'
Assert-Equal (Invoke-ResolvedGit @('hash-object', $baseLock)) '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9'
Assert-Equal (Invoke-ResolvedGit @('hash-object', $ctutilsLock)) 'fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66'
Assert-Equal (Invoke-ResolvedGit @('hash-object', $candidateLock)) 'c4b4316368317a342058f77151ab8043afea676b'
Write-RawGitOutput $mergedLock @('merge-file', '-p', '--', $packageLock, $baseLock, $ctutilsLock)
$constructedLockBlob = Invoke-ResolvedGit @('hash-object', $mergedLock)
Assert-Equal $constructedLockBlob 'c4b4316368317a342058f77151ab8043afea676b'
Assert-Equal $constructedLockBlob (Invoke-ResolvedGit @('hash-object', $candidateLock))
'@
    $mergeOnlyPortable = $resolverHappy + "`n" + $rawCaptureHappy + "`n" + $compositionHappy
    Assert-VerifierScriptRecursionSafety $mergeOnlyPortable
    Assert-LockCompositionContract $mergeOnlyPortable
    foreach ($missingSourceHash in @(
        "Assert-Equal (Invoke-ResolvedGit @('hash-object', `$packageLock)) '9e36a2e89363eeedc4215a68dbd51f33dfec3741'",
        "Assert-Equal (Invoke-ResolvedGit @('hash-object', `$baseLock)) '47fbe5113e4b7c18cf2cc96f3a0410f045a281a9'",
        "Assert-Equal (Invoke-ResolvedGit @('hash-object', `$ctutilsLock)) 'fdb8d4c8fe35e68650b80f2c3e7ccb69b56b1f66'",
        "Assert-Equal (Invoke-ResolvedGit @('hash-object', `$candidateLock)) 'c4b4316368317a342058f77151ab8043afea676b'"
    )) {
        Assert-Throws { Assert-RawBinaryCaptureContract ($mergeOnlyPortable.Replace($missingSourceHash, '')) } "raw capture without its source-byte hash must fail: $missingSourceHash"
    }
    $noMergedHash = $mergeOnlyPortable.Replace("Assert-Equal `$constructedLockBlob 'c4b4316368317a342058f77151ab8043afea676b'", '')
    Assert-Throws { Assert-RawBinaryCaptureContract $noMergedHash } 'raw emitted merge bytes without final hash binding must fail'
    $textArguments = $mergeOnlyPortable.Replace('[void]$startInfo.ArgumentList.Add($argument)', '$startInfo.Arguments = $argument')
    Assert-Throws { Assert-RawBinaryCaptureContract $textArguments } 'raw capture helper must reject hand-built text Arguments command lines'
    Assert-Throws { Assert-RawBinaryCaptureContract ($mergeOnlyPortable + "`nStart-Process -FilePath `$gitExecutablePath -RedirectStandardOutput `$mergedLock") } 'Start-Process redirect capture must fail the raw binary contract'
    foreach ($textCapture in @(
        '$bytes | Out-String | Set-Content $mergedLock',
        'Add-Content -Path $mergedLock -Value $text',
        '$text > $mergedLock',
        'Set-Content -Path $mergedLock -Encoding utf8 -Value $text'
    )) {
        Assert-Throws { Assert-RawBinaryCaptureContract ($mergeOnlyPortable + "`n" + $textCapture) } "text or encoding capture must fail the raw binary contract: $textCapture"
    }
    $wrongOptions = $mergeOnlyPortable.Replace("@('merge-file', '-p', '--', `$packageLock, `$baseLock, `$ctutilsLock)", "@('merge-file', '--diff3', '--', `$packageLock, `$baseLock, `$ctutilsLock)")
    Assert-Throws { Assert-RawBinaryCaptureContract $wrongOptions } 'wrong merge options must fail the raw binary contract'
    $dynamicChoice = $mergeOnlyPortable.Replace("Write-RawGitOutput `$mergedLock @('merge-file', '-p', '--', `$packageLock, `$baseLock, `$ctutilsLock)", "`$mergeArguments = @('merge-file', '-p', '--', `$packageLock, `$baseLock, `$ctutilsLock)`nWrite-RawGitOutput `$mergedLock `$mergeArguments")
    Assert-Throws { Assert-RawBinaryCaptureContract $dynamicChoice } 'dynamic merge argument choice must fail the raw binary contract'
    foreach ($replay in @(
        '& $gitExecutablePath apply --no-index $packagePatch',
        "Start-Process -FilePath `$gitExecutablePath -ArgumentList @('apply', '--no-index', `$packagePatch)",
        "Invoke-ResolvedGit @('patch', `$packagePatch)",
        "Invoke-ResolvedGit @('am', `$packagePatch)",
        "Invoke-ResolvedGit @('diff', '--binary', 'HEAD')"
    )) {
        Assert-Throws { Assert-LockCompositionContract ($mergeOnlyPortable + "`n" + $replay) } "lock replay must fail the composition contract: $replay"
    }
}

Invoke-Fixtures
Invoke-StaticWorkflowFixtures
Invoke-VerifierPortabilityFixtures
$pendingFixtureResult = Get-RawImmutableFixtureResult $false
Assert ($pendingFixtureResult.State -eq 'PENDING' -and $pendingFixtureResult.ExitCode -eq 2) 'unavailable raw immutable-blob fixture capability must be PENDING with exit 2'
$readyFixtureResult = Get-RawImmutableFixtureResult $true
Assert ($readyFixtureResult.State -eq 'READY' -and $readyFixtureResult.ExitCode -eq 0) 'available raw immutable-blob fixture capability must remain executable'
$defaultPendingResult = Get-DefaultContractCapabilityResult $false
Assert ($defaultPendingResult.ReportPending -and $defaultPendingResult.ContinueStaticContracts) 'default route must report unavailable raw capture as PENDING and continue static contracts'
$defaultReadyResult = Get-DefaultContractCapabilityResult $true
Assert ((-not $defaultReadyResult.ReportPending) -and $defaultReadyResult.ContinueStaticContracts) 'default route must run static contracts when raw capture is executable'
Invoke-LockCompositionFixtures
if ($FixturesOnly) {
    if ($script:RawBinaryFixturePending) {
        Write-Host "PENDING: raw immutable-blob fixture: $script:RawBinaryFixturePendingReason"
        exit 2
    }
    Write-Host 'PASS: candidate-verifier workflow contracts and hermetic fixtures'
    exit 0
}
# The default route intentionally evaluates these static contracts even when the
# local raw-byte fixture is PENDING; only FixturesOnly returns early for that
# capability state.
Assert-WorkflowContract (Join-Path $PSScriptRoot '..\.github\workflows\candidate-verifier.yml')
$verifierScript = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'verify-package-candidate.ps1'))
Assert-VerifierScriptPortability $verifierScript
Assert-VerifierScriptRecursionSafety $verifierScript
Assert-LockCompositionContract $verifierScript
if ($script:RawBinaryFixturePending) {
    Write-Host "PENDING: raw immutable-blob fixture after completed static contract evaluation: $script:RawBinaryFixturePendingReason"
    exit 2
}
Write-Host 'PASS: candidate-verifier workflow contracts and hermetic fixtures'
