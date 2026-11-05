# Reference: git commands & merge stages

## Useful commands

```bash
git status
git diff --name-only --diff-filter=U
git diff path/to/file
git show :2:path/to/file   # ours (HEAD) during merge
git show :3:path/to/file   # theirs (MERGE_HEAD) during merge
git log --oneline -5 HEAD MERGE_HEAD
```

## Abort

```bash
git merge --abort
git rebase --abort
```

## After user approves resolution

```bash
git add <resolved-files>
git commit   # completes merge; use message that states target branch
```

## Suggested merge commit message shape

```
Merge origin/main into <branch>

<one line: what was integrated>

Conflict resolutions: <brief list or "per user direction in chat">.
```
