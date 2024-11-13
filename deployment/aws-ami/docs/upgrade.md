

### AMI Upgrade

You can update AMI when new Syndicate Rule Engine [release](https://github.com/epam/syndicate-rule-engine/releases) on GitHub is available.
To check new releases from AMI use this command:
```bash
sre-init list
```
The command will display only the current release and new releases if those are available.


To update Syndicate Rule Engine to the next release use this command:
```bash
sre-init update --yes
```
**Note:** no prompt will be shown if you specify `--yes` flag.

If you just want to check if new release is available use this command:
```bash
sre-init update --check
```
The command will have 0 status code if update is not available and 1 status code otherwise.


To update Defect Dojo installation use:

```bash
sre-init update --defectdojo
```

In case update fails it will be rolled back to the previous version of the application.
