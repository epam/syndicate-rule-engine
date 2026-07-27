# Update Guide

The update process is fully automated. EPAM Syndicate Rule Engine uses an **incremental upgrade flow** — it is only possible to update through each successive version; skipping intermediate versions is not supported.

All commands must be executed **directly on the SRE AMI instance** via SSH.

---

### 1. Connect to the instance via SSH

```bash
ssh -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS
```

- `$SSH_KEY_NAME` — the name of your SSH key file
- `$INSTANCE_PUBLIC_DNS` — the public DNS of the instance

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

---

### 3. Check whether an update is available

```bash
sre-init update --check
```

- Returns `Up-to-date` with exit code `0` if no update is available.
- Returns exit code `1` if a new release is found — useful for automation.

---

### 4. Refresh the update manager

Before performing the upgrade, ensure `sre-init` itself is up to date:

```bash
sre-init update --same-version --no-backup --no-patch
```

Expected output:

```
Automatically updated sre-init from <current_version> to <new_version>
```

---

### 5. Perform the update

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
Updating sre-init
Done
```

> **Note:** The helm upgrade step may take up to 20 minutes. Do not interrupt the process.  
> The update is fail-safe — if anything goes wrong, `sre-init` will automatically roll back all changes to the previous state.

---

### 6. Verify the installation health

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

---

### 7. Defect Dojo update (optional)

To update Defect Dojo separately:

```bash
sre-init update --defectdojo
```

> This update is fail-safe as well.

---

### Troubleshooting

If the update fails or any health check reports `failed`, collect the log file from the instance and contact our support team:

```bash
scp -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS:/var/log/sre-init.log /your/local/directory/
```

Contact: [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com)