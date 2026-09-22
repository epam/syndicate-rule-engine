# Update Guide

The update process is fully automated. EPAM Syndicate Rule Engine uses an **incremental upgrade flow** — it is only possible to update through each successive version; skipping intermediate versions is not supported.

All commands must be executed **directly on the SRE AMI instance** via SSH.

---

## Prerequisites

Before starting the upgrade, ensure the following conditions are met:

- **Instance is healthy** — run `sre-init health` and confirm all checks show `ok`
- **Disk space** — at least 5 GB of free disk space available (`df -h /`)
- **GitHub connectivity** — the instance must be able to reach `https://github.com` to pull release artifacts
- **Active SSH session** — do not close the SSH session during the upgrade; consider using `screen` or `tmux` to protect against disconnection:
  ```bash
  screen -S sre-update
  ```
  To reattach after disconnection: `screen -r sre-update`

> **Downtime notice:** The helm upgrade step causes a brief service interruption (typically under 5 minutes). Plan accordingly.

---

### 1. Connect to the instance via SSH

```bash
ssh -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS
```

- `$SSH_KEY_NAME` — the name of your SSH key file
- `$INSTANCE_PUBLIC_DNS` — the public DNS of the instance

See the [Access Guide](access.md) for detailed SSH setup instructions.

---

### 2. List available releases

Run the following command to see all releases available from the currently installed version:

```bash
sre-init list
```

Expected output:

| Version | Release Date         | URL                                                                 | Prerelease | Draft |
|---------|----------------------|---------------------------------------------------------------------|------------|-------|
| 5.5.1   | 2024-11-01T10:00:00Z | https://github.com/epam/syndicate-rule-engine/releases/tag/5.5.1   | false      | false |
| 5.5.0*  | 2024-10-16T09:01:13Z | https://github.com/epam/syndicate-rule-engine/releases/tag/5.5.0   | false      | false |

The currently installed version is marked with an asterisk `*`.

> **Multiple versions behind?** If several versions are listed, run `sre-init update` once per version in order. Each run upgrades exactly one version.

---

### 3. Check whether an update is available

```bash
sre-init update --check
```

- Returns `Up-to-date` with exit code `0` if no update is available.
- Returns exit code `1` if a new release is found — useful for automation.

---

### 4. Create a backup

Although the update is fail-safe, it is strongly recommended to create a manual backup before proceeding:

```bash
sre-init backup create --name pre-upgrade-$(date +%Y%m%d)
```

To list existing backups:

```bash
sre-init backup ls
```

---

### 5. Refresh the update manager

Before performing the upgrade, ensure `sre-init` itself is up to date:

```bash
sre-init update --same-version --no-backup --no-patch
```

Expected output:

```
Automatically updated sre-init from <current_version> to <new_version>
```

---

### 6. Perform the update

```bash
sre-init update
```

When prompted, confirm by typing `y`:

```
Do you want to update? [y/N] y
```

> Use `sre-init update --yes` to skip the confirmation prompt.

The command logs progress to the console. Expected output upon successful completion:

```
The current installed version is <previous_version>
New github release <new_version> is available
Going to update to <new_version>
Pulling new artifacts
Verifying that necessary helm chart exists
Making helm upgrade. It should not take more than 20 minutes
helm upgrade was successful
Upgrading obfuscation manager
Upgrading modular CLI
Refreshing CLIs for other onboarded users: user1 userN
Updating sre-init
Done
```

> **Note:** The helm upgrade step may take up to 20 minutes. Do not interrupt the process.  
> The update is fail-safe — if anything goes wrong, `sre-init` will automatically roll back all changes to the previous state.  
> In addition to the current user, `sre-init update` also refreshes `modular-cli`/`sre-obfuscator` for every other
> linux user onboarded via [`sre-init init --user`](main.md#213-initialize-ami-for-another-linux-user) (auto-detected).
> A refresh failure for one user only warns and does not fail the update. Use `--no-secondary-user-clis` to skip this
> step, or run `sre-init update cli` at any time to refresh CLIs independently of a full update.

---

### 7. Verify the installation health

After the update completes, confirm all components are running correctly:

```bash
sre-init health
```

Expected output — all checks should show `ok`:

```
№  CHECK                               STATUS
1  /usr/local/sre/.success             ok
2  Syndicate Rule Engine helm release  ok
3  Syndicate entrypoint                ok
4  Syndicate Rule Engine health check  ok
5  Obfuscation manager entrypoint      ok
6  Defect Dojo helm release            ok
```

To confirm the installed version after the upgrade:

```bash
sre-init version
```

---

### 8. Defect Dojo update (optional)

To update Defect Dojo separately:

```bash
sre-init update --defectdojo
```

> This update is fail-safe as well.

---

## Rollback

The update process performs an automatic rollback if the helm upgrade fails. If the automatic rollback does not resolve the issue, restore from the backup you created in Step 4:

```bash
sre-init backup restore --name pre-upgrade-<date>
```

To list available backups:

```bash
sre-init backup ls
```

---

## Troubleshooting

If the update fails or any health check reports `failed`, collect the log file from the instance and contact our support team:

```bash
scp -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS:/var/log/sre-init.log /your/local/directory/
```

Contact: [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com)
